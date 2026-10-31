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
| body | 512 KiB, with or without `Content-Length` | `413` |
| spans per request | 512 | `413` |
| attributes per span | 64 (only 3 are forwarded; the rest are dropped) | `413` |
| resource attributes | 64 | `413` |
| resource spans / scope spans per request | 16 / 64 | `413` |
| a key repeated within any JSON object | refused | `400` |
| requests per user | 1/s sustained, bursts of 10 | `429` with `Retry-After` (seconds) |
| requests in flight per user | 1 | `429` with `Retry-After: 1` |
| decodes in flight, all users | GOMAXPROCS of the pod | `429` with `Retry-After: 1` |
| spans into the collector, all users | 2000/s, bursts of 2048 | `429` with `Retry-After` |

- **Structural limits:** the span, attribute and container caps are enforced *while* the body is decoded. The decoder stops at cap+1, so what a request allocates is set by the caps, not by the shape of the body.
- **Per-user limit:** one user hitting it does not affect another. The browser exporter's default 5 s batching uses about 0.2 requests/s per tab.
- **Repeated keys:** a JSON key that appears twice in one object (a span, a resource, a resource span, a scope span, an attribute or its value) gets `400`. A repeat used to restart the attribute count (#1020 delta D1).
- **What remains, by design** (#1020 delta D2/D3):
  - **Decode slots:** one user can hold at most one of them. But GOMAXPROCS *distinct* authenticated users sending slow bodies together can still hold every slot. That blocks **telemetry only** (others get `429`), never the portal. Each slot is bounded by the server's 10 s read timeout.
  - **Span budget:** the per-user limit counts *requests*, not spans. So a few users at their limit with full 512-span batches can use up the shared 2000 spans/s, and other users' batches get `429` until it refills.
- **Attribution:** every refusal (400, 413, 415, 429) writes a sampled WARN line `rum.ingest.refused` with the caller's `redact.User` label (never the username in clear), the status and the reason.

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
| `http.route` | on `krateo.widget.fetch`: exactly one of snowplow's registered routes. On the page-level spans: a route **template** (`/compositions/{namespace}/{name}`) that starts with `/`, uses only `A-Z a-z 0-9 / _ . : { } -`, is at most 256 characters, and has no segment of 24 characters or more and no JWT-shaped segment; a query string, fragment or `%`-escape fails the character check. See the contract below. |
| `http.request.method` | `GET`, `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS` (upper case) |
| `http.response.status_code` | an integer from 100 to 599 (`intValue`) |

Every other attribute is dropped and counted, as is an allowlisted key whose value fails its check. That includes `enduser.id`, `url.full` and any header.

### `http.route`: the frontend contract

Diego's ruling (2026-10-30, #1020 F3): **exact where possible, trust the rest.**

| span | what `http.route` must be | what snowplow checks |
|---|---|---|
| `krateo.widget.fetch` | the **snowplow route** the widget called: `/call`, `/call/read`, `/export`, … with no query string | **Exact:** the route must be one of snowplow's own registered routes, looked up on snowplow's live routing table (never a hand-written list). Anything else is dropped and counted in `snowplow_rum_ingest_widget_routes_unregistered_total` |
| `krateo.page.load`, `krateo.composition.open`, `krateo.form.submit`, `krateo.page.publish` | the **pattern your router matched**, e.g. `/compositions/:namespace/:name` or `/compositions/{namespace}/{name}` | **Trusted as sent,** with today's checks: no query string, fragment, escape or space; no segment of 24+ characters; no JWT-shaped segment |

**Never send `location.pathname`.** Send the router's matched pattern.
- **Why:** a concrete path such as `/compositions/team-a/prod-db` carries names.
- **The limit of the page-span checks:** they cannot catch a short, word-shaped name, because `/users/alice/profile` and `/users/settings/profile` look the same to any syntax rule.
- **So:** for page spans, the frontend sending the matched pattern is the guarantee.

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
| `snowplow_rum_ingest_requests_total` | `accepted`, `dropped_auth`, `dropped_size`, `dropped_rate`, `dropped_content_type`, `dropped_malformed`, `dropped_busy` |
| `snowplow_rum_ingest_spans_total` | `accepted`, `dropped_service`, `dropped_name`, `dropped_invalid`, `forwarded`, `forward_failed`, `forward_overflow`, `forward_rate_limited` |
| `snowplow_rum_ingest_attributes_dropped_total` | (none) |
| `snowplow_rum_ingest_widget_routes_unregistered_total` | (none): `krateo.widget.fetch` routes that are not a snowplow route (a subset of the above) |

No audit event is written: this is telemetry, not a mutation.
