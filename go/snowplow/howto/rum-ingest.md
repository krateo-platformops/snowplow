# Browser RUM ingest: `POST /telemetry/v1/traces`

This is for the portal frontend (#1015, acmp X5).

The portal sends its browser RUM spans to **snowplow**, authenticated as the signed-in user. It never sends them to a collector route: the collector stays ClusterIP-only. snowplow keeps only the allowlisted fields, rebuilds the payload, and forwards it to the same OTel collector its own spans go to.

The portal's `krateo.widget.fetch` spans link to snowplow's `Server` spans (#1013) through the W3C `traceparent` the fetch carries.

## Endpoint

| | |
|---|---|
| URL | `<snowplow base URL>/telemetry/v1/traces`: the same base the portal uses for `/call` (on 057: `<origin>/content/telemetry/v1/traces`) |
| Method | `POST` |
| Content type | `application/json`, **OTLP/JSON only** (`@opentelemetry/exporter-trace-otlp-http`). Protobuf (`application/x-protobuf`) gets `415`. Send no `Content-Encoding`. |
| Auth | `Authorization: Bearer <JWT>`, the same header as `/call`. Without a valid one: `401`. |
| CORS | The preflight allows `Authorization`, `Content-Type`, `traceparent`, `tracestate` and `baggage` (`snowplowCORSOptions`, every route). |
| Enabled | Only when snowplow's own tracing is on (`OTEL_TRACING_ENABLED`, defaulting to `OTEL_ENABLED`) **and** `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Otherwise the route answers `404` to everyone. |

Exporter configuration, as a sketch:

```js
new OTLPTraceExporter({
  url: `${snowplowBase}/telemetry/v1/traces`,
  headers: { Authorization: `Bearer ${jwt}` },
});
// BatchSpanProcessor: keep maxExportBatchSize <= 512 (the default).
```

## Limits

| limit | value | answer |
|---|---|---|
| body | 512 KiB | `413` |
| spans per request | 512 | `413` |
| requests per user | 1/s sustained, bursts of 10 | `429` with `Retry-After` (seconds) |

The limit is per authenticated user, not global: one user hitting it does not affect another. The browser exporter's default 5 s batching uses about 0.2 requests/s per tab.

## Allowlist

Everything not listed here is dropped. Nothing is forwarded as sent: the payload is rebuilt from the fields below.

- **Resource:** `service.name` must be exactly `krateo-frontend`. Any other value drops the whole resource's spans. No other resource attribute is forwarded: the forwarded resource carries `service.name=krateo-frontend` only. Scope is set by snowplow to `snowplow/rum-ingest`.
- **Span names:** `krateo.page.load`, `krateo.composition.open`, `krateo.form.submit`, `krateo.page.publish`, `krateo.widget.fetch`. Any other name is dropped.
- **Span fields kept:**
  - `traceId`, `spanId`, `parentSpanId`, as hex, the OTLP/JSON encoding;
  - `kind`;
  - `startTimeUnixNano` and `endTimeUnixNano` (the duration);
  - `status.code`. `status.message` is dropped.
  - Events, links and trace state are dropped.
  - A span with a malformed id or time is dropped.
- **Span attributes kept:**

| key | allowed values |
|---|---|
| `http.route` | a route **template** (`/call`, `/compositions/{namespace}/{name}`): starts with `/`, only `A-Z a-z 0-9 / _ . : { } -`, at most 256 characters, no segment of 24 characters or more and no JWT-shaped segment. A query string, fragment or `%`-escape fails the character check. |
| `http.request.method` | `GET`, `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS` (upper case) |
| `http.response.status_code` | an integer from 100 to 599 (`intValue`) |

Every other attribute is dropped and counted, as is an allowlisted key whose value fails its check. That includes `enduser.id`, `url.full` and any header.

**Spans carry no identity.** snowplow uses the authenticated user only for the rate limit and never adds it to a span.

## Response

- `200` with `{}` when every span was forwarded.
- `200` with `{"partialSuccess":{"rejectedSpans":"<n>","errorMessage":"…"}}` when spans were dropped by the allowlist or because the forward queue was full.
- `400` for a body that is not OTLP/JSON, plus the codes above.

Forwarding is asynchronous and bounded (a 64-batch queue, one worker, 5 s per post). A slow or absent collector never delays the response: what cannot be queued is dropped and counted.

## Metrics

On `/debug/vars` as `snowplow_rum_ingest`, and over OTLP:

| metric | `outcome` values |
|---|---|
| `snowplow_rum_ingest_requests_total` | `accepted`, `dropped_auth`, `dropped_size`, `dropped_rate`, `dropped_content_type`, `dropped_malformed` |
| `snowplow_rum_ingest_spans_total` | `accepted`, `dropped_service`, `dropped_name`, `dropped_invalid`, `forwarded`, `forward_failed`, `forward_overflow` |
| `snowplow_rum_ingest_attributes_dropped_total` | (none) |

No audit event is written: this is telemetry, not a mutation.
