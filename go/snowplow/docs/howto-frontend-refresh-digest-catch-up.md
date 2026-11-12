# Frontend guide: catching up on arm with `X-Snowplow-Refresh-Digest` and `held` (#1324)

This extends the live-refresh contract in
[howto-frontend-live-refresh-sse.md](howto-frontend-live-refresh-sse.md). It is
additive: a client that ignores it behaves exactly as today.

## 1. The problem it closes

`/refreshes` keeps no replay. A `refresh` frame published while a widget's key
is not armed on any open stream reaches nobody. That happens in two windows:

1. between a widget's `/call` and the moment its key is armed, and
2. every time the SPA re-opens its single stream, which it does on **every**
   widget mount and unmount (`scheduleReconnect` aborts the stream and
   reconnects with the full armed set, 200 ms debounced, deliberately without
   re-validating). Every armed key is exposed during each re-open, not only
   the new one.

Until now a periodic identical republish hid the loss. With #1321 snowplow
publishes a frame only when the bytes change, so a frame lost in one of these
windows stays lost until the next real change. The fix: the tab tells snowplow
what it holds when it arms, and snowplow answers with one frame for every key
it is behind on.

## 2. The response header

Every armed `/call` response (one that carries `X-Snowplow-Refresh-Key`) also
carries:

```
X-Snowplow-Refresh-Digest: v1.<32 lowercase hex>
```

- It is an **opaque string**. Store it and echo it; never parse it and never
  compute it yourself. The `v1.` prefix names the algorithm. If snowplow ever
  changes it, every held digest simply differs once (one extra refetch per
  widget), never matches by accident.
- It names **exactly the bytes of that response body**, the bytes this tab
  rendered, plus (#1319) the `X-Snowplow-Stage-Outcomes` the response carried
  when a stage did not succeed. A complete answer's digest is the body's
  alone, as before. An answer with incomplete stages digests differently, so a
  cell that later completes with the same body still catches the tab up
  ([howto-frontend-widget-stage-outcomes.md](howto-frontend-widget-stage-outcomes.md)
  section 6). For the identity-free `widgetContent` class the body is narrowed per
  user (`status.resourcesRefs.items[].allowed`), so two users of the same key
  get different digests. That is intended: each tab is compared with what *its*
  `/call` would answer now.
- It is stamped only together with `X-Snowplow-Refresh-Key`. No key means no
  digest.
- It is listed in CORS `Access-Control-Expose-Headers`, so a cross-origin
  `fetch` can read it like `X-Snowplow-Refresh-Key` and
  `X-Snowplow-Refresh-Class`.

## 3. The arm field `held`

Each coordinate in the `/refreshes` subscription (the `?sub=` base64 JSON array,
or the POST body) gains one optional field:

| field  | type   | meaning |
|--------|--------|---------|
| `held` | string | the `X-Snowplow-Refresh-Digest` of the most recent `/call` response this widget rendered |

On subscribe, snowplow computes for each armed key with a `held` the digest of
what that widget's `/call` would answer **now, for this user**. If it differs,
or the cell is gone, the stream gets **exactly one** ordinary
`event: refresh` / `data: <key>` frame for that key, right after connect. Every
other key gets nothing. Handle it like any other `refresh` frame: refetch
through your normal queue. A gone cell's refetch is a fill, and a 404 then
follows the rules in §10 of the live-refresh guide.

Send `held` on **every** (re)connect, including the mount/unmount re-opens.
That alone closes window 2 without any re-validation burst: only behind
widgets refetch.

### Example (pinned to the wire by `TestRefreshes1324_DocEqualsWire`)

A `/call` answered:

```http
HTTP/1.1 200 OK
Content-Type: application/json
X-Snowplow-Refresh-Key: 3f2a9c…
X-Snowplow-Refresh-Class: widgets
X-Snowplow-Refresh-Digest: v1.0a8f3f6d7eb6f73c0be6c50db2744fb0

{"apiVersion":"widgets.templates.krateo.io/v1beta1","kind":"Panel","metadata":{"name":"dashboard","namespace":"krateo-system"},"status":{"widgetData":{"title":"Dashboard"}}}
```

The widget then arms with the digest it holds:

```json
[{"class":"widgets","group":"widgets.templates.krateo.io","version":"v1beta1","resource":"panels","namespace":"krateo-system","name":"dashboard","held":"v1.0a8f3f6d7eb6f73c0be6c50db2744fb0"}]
```

## 4. The dedup caveat: keep `held` out of the coords equality

The SPA decides whether a refetch must re-arm by comparing the recorded
`{coords, key}` with the previous one (`recordRefreshHeaders`:
`prev.key === key && JSON.stringify(prev.coords) === JSON.stringify(coords)`).
The digest changes on every refetch that brought new bytes. If it were part
of `coords`, every such refetch would look like a new subscription and re-open
the stream: the amplification the SPA is built to avoid.

So store the digest **beside** the coords, never in them. The `RefreshEntry`
object also must not be replaced when only the digest changes: its identity is
the arm hook's effect dependency (`useWidgetLiveRefresh`: `[enabled, widgetId,
entry, …]`), and a new object would re-arm. Two options:

- a separate map, widgetId (or refresh key) → latest digest, written on every
  `/call` response;
- a mutable field updated in place on the existing entry.

Add the digest to each coordinate only when building the subscription payload
in `connect()`. It must never affect whether a re-arm happens.

## 5. Backward compatibility

- **No header, no `held`.** If a `/call` response carries no
  `X-Snowplow-Refresh-Digest` (an older snowplow, or an uncached or unarmed
  path), send no `held` for that widget. Do not send a stale digest from an
  earlier response for a different key.
- **Absent or `""` means no catch-up.** snowplow treats a coordinate without
  `held`, or with `"held": ""`, exactly as today: it is armed and nothing is
  checked.
- **Older snowplow ignores `held`.** It decodes the coordinate array with
  `encoding/json`, which ignores unknown fields, so a newer SPA can send `held`
  to an older server safely.
- **Coordinate order and key derivation are unchanged.** `held` is never part
  of the key.

## 6. Observability (snowplow side)

`/debug/vars` → `snowplow_refresh_broadcaster` (OTLP
`snowplow_refresh_broadcaster_<stat>_total`):

- `arm_held`: armed keys whose arm carried a `held` digest. Zero means no tab
  sends it yet.
- `arm_catch_up`: catch-up frames sent because the tab was behind.
