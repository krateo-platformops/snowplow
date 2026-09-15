# Frontend guide — integrating snowplow's live-refresh SSE (per widget)

**Audience:** Krateo portal frontend. **Snowplow feature:** live-refresh-coherence (Ship 1).
**Status:** implemented + released on the 1.5.x line; default-ON when the cache is on.

---

## 1. What it gives you

When a cluster object behind a widget changes, snowplow's cache re-resolves the
affected entry in the background and then **pushes a one-line signal** to the
browser saying *"this widget's data is fresh — refetch it."* You get live UI
updates **without polling**, and the refetch is a warm cache **hit** (no apiserver
load).

It is **signal-only**: the SSE event carries *just a cache key*, never data. You
always get the actual content from a normal `GET /call` (which re-applies the
user's RBAC at serve time). So the stream can be shared/coalesced without ever
leaking another user's row-level visibility.

```
object changes ─▶ snowplow informer ─▶ dirty-mark ─▶ background re-resolve
                                                          │
   browser ◀── event: refresh\ndata: <l1Key> ◀───────────┘   (GET /refreshes)
      │
      └─▶ GET /call (same widget)  ──▶ warm L1 HIT, fresh content ──▶ update UI
```

---

## 2. The contract (what to build against)

### 2.1 Endpoint
```
GET /refreshes?sub=<base64-url(JSON coordinate array)>
Content-Type: text/event-stream
```
One **multiplexed** stream **per browser tab** — *not* one per widget. You arm all
the widgets the tab has mounted on a single connection.

### 2.2 Auth — cookie (EventSource can't set headers)
A browser `EventSource` cannot set the `Authorization` header, so snowplow reads
the JWT from a **session cookie**:
```js
new EventSource(url, { withCredentials: true })   // sends the session cookie
```
- Cookie name is deploy-configurable (`REFRESH_SESSION_COOKIE`, default
  **`krateo-session`**). Confirm the portal's actual session-cookie name with ops.
- The token is **never** put in the URL (it would leak in logs/referrer).
- Non-browser clients (tests, a polyfill) may instead send `Authorization: Bearer <jwt>`.
- Cross-origin? The snowplow CORS config must allow credentials.

### 2.3 Arming — send widget **coordinates**, not keys
`?sub=` is base64 of a JSON **array**, one object per widget you want notified
about. **You send coordinates; snowplow derives the cache key under the
authenticated identity** (you can't subscribe to someone else's key — it's
forgery-proof by construction):

```jsonc
[
  {
    "class": "widgets",          // the X-Snowplow-Refresh-Class from /call — see §2.5
    "group": "widgets.templates.krateo.io",
    "version": "v1beta1",
    "resource": "barcharts",     // the widget's GVR…
    "namespace": "demo",
    "name": "cpu-by-node",       // …and name — exactly as in your /call
    "perPage": 0,
    "page": 0,
    "extras": { "compositionId": "fsa-y8" }   // same extras you passed to /call
  }
]
```
**The coordinates must match the widget's `/call` exactly** (same GVR, ns, name,
page, extras) — that's what makes the derived key equal the key the event will
carry. Limits: **≤ 512 widgets** per connection, **≤ 16 KB** decoded `sub`.

### 2.4 Matching events back to widgets — the `X-Snowplow-Refresh-Key` + `X-Snowplow-Refresh-Class` headers
Every `GET /call` response that was cache-keyed carries TWO headers:
```
X-Snowplow-Refresh-Key:   <l1Key>
X-Snowplow-Refresh-Class: <class>   # widgets | widgetContent | restactions
```
- `X-Snowplow-Refresh-Key` is the exact key this widget's refresh events will
  arrive under. **Store `l1Key → widget` when you render**, then match incoming
  events by it.
- `X-Snowplow-Refresh-Class` is the class snowplow actually keyed this response
  under. **Arm the subscription with this class verbatim** — no guessing. (You
  arm by *coordinates + class*; you match by *key*. Both resolve to the same
  `l1Key`.)

Both headers are additive and only present when the response was cache-keyed
(absent on a cache-off / RBAC-skipped / identity-less response — in which case
there is nothing to arm). They are in the CORS `ExposedHeaders` list so a
cross-origin fetch can read them.

### 2.5 `class` — read it from the response, don't guess
**Send back the value of `X-Snowplow-Refresh-Class` from the `/call` response.**
That is the class snowplow keyed the response under, so it is always the
armable one:

| You rendered | `X-Snowplow-Refresh-Class` you'll get back |
|---|---|
| a RESTAction (`/call?resource=<ra>`) | `restactions` |
| an RBAC-*sensitive* widget (`/call?resource=<widget>`) | `widgets` |
| an RBAC-*insensitive* widget (shared shell) | `widgetContent` |

This resolves the earlier widgets-vs-widgetContent ambiguity: RBAC-insensitive
widgets are served from a shared shell keyed under `widgetContent`, and the
header now tells you that directly — no need to arm both classes. Other internal
classes (`apistage`, `raFullList`) are never stamped on a `/call` response, so
the frontend never arms them.

### 2.6 The event frames
```
event: refresh
data: <l1Key>

: keepalive
```
- **`event: refresh`** is a **named** event → listen with
  `es.addEventListener('refresh', …)`, **not** `es.onmessage`.
- `: keepalive` is an SSE **comment** (every 20 s) — `EventSource` ignores it; it
  just keeps the connection alive. You don't handle it.

---

## 3. Per-widget integration flow

1. **On render**, call `GET /call` for the widget as you do today. Read the
   **`X-Snowplow-Refresh-Key`** response header → record `key → widgetId`, and
   the **`X-Snowplow-Refresh-Class`** header → use as this widget's `class`.
2. **Collect coordinates** for every mounted widget (the same params you passed to
   `/call`), using the `class` from each widget's `X-Snowplow-Refresh-Class`.
3. **Open one `EventSource`** for the tab:
   `GET /refreshes?sub=<base64url(coordsArray)>` with `withCredentials: true`.
4. **On `refresh`** (`addEventListener('refresh', …)`): `e.data` is an `l1Key` →
   look up the widget(s) with that key → **refetch their `/call`** (warm hit, fresh
   data) → update the UI.
5. **Throttle** the refetch **per widget (~5 s)**. Snowplow already coalesces
   duplicate signals server-side (250 ms window) and may drop bursts by design, so
   treat a `refresh` as *"data changed, refetch when convenient"*, not *"refetch
   instantly every time."*
6. **On mount/unmount changes**, rebuild `sub` and re-open the connection (close
   the old one). It's cheap; keep it to one connection per tab.

---

## 4. Reference implementation (TypeScript)

```ts
type Coords = {
  class: 'widgets' | 'widgetContent' | 'restactions';
  group: string; version: string; resource: string;
  namespace: string; name: string;
  perPage?: number; page?: number;
  extras?: Record<string, unknown>;
};

const b64url = (s: string) =>
  btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

class RefreshManager {
  private es?: EventSource;
  private armed = new Map<string, Coords>();        // widgetId -> coords
  private keyToWidgets = new Map<string, Set<string>>(); // l1Key  -> widgetIds
  private lastRefetch = new Map<string, number>();  // widgetId -> ts (throttle)

  /** Call right after a widget's /call resolves. */
  onWidgetRendered(widgetId: string, coords: Coords, refreshKeyHeader: string | null) {
    this.armed.set(widgetId, coords);
    if (refreshKeyHeader) {
      let set = this.keyToWidgets.get(refreshKeyHeader);
      if (!set) this.keyToWidgets.set(refreshKeyHeader, (set = new Set()));
      set.add(widgetId);
    }
    this.reconnect(); // re-arm the (single) stream with the new widget set
  }

  onWidgetUnmounted(widgetId: string) {
    this.armed.delete(widgetId);
    for (const set of this.keyToWidgets.values()) set.delete(widgetId);
    this.reconnect();
  }

  private reconnect() {
    this.es?.close();
    const coords = [...this.armed.values()];
    if (coords.length === 0) return;
    const sub = b64url(JSON.stringify(coords));
    this.es = new EventSource(`/refreshes?sub=${sub}`, { withCredentials: true });
    this.es.addEventListener('refresh', (e) => this.onRefresh((e as MessageEvent).data));
    // EventSource auto-reconnects on drop and re-sends this same URL; no extra code.
  }

  private onRefresh(l1Key: string) {
    const widgets = this.keyToWidgets.get(l1Key);
    if (!widgets) return;
    const now = Date.now();
    for (const id of widgets) {
      if (now - (this.lastRefetch.get(id) ?? 0) < 5000) continue; // 5s/widget throttle
      this.lastRefetch.set(id, now);
      this.refetchWidget(id); // your existing /call refetch -> update UI
    }
  }

  // refetchWidget: re-issue the widget's GET /call; it returns a warm L1 hit
  // with the fresh content, and a fresh X-Snowplow-Refresh-Key (re-record it).
}
```

---

## 5. Graceful degradation & edge cases

- **Feature off / cache off** → `/refreshes` returns a clean **idle stream**
  (heartbeats only, *zero* refresh events). Your code simply never fires a
  `refresh` → fall back to your own polling/throttle. No errors, no hangs.
- **`400`** → malformed/oversized `sub` (bad base64, > 512 entries, > 16 KB).
- **Coordinates the user can't access** are **silently skipped** (fail-closed) —
  you won't get events for them; that's correct, not an error.
- **Always refetch via `/call`** on a signal — never treat the event `data` as
  content. RBAC is re-applied at `/call` serve time.
- **One connection per tab.** Arming 512 widgets on one stream is fine; opening
  512 EventSources is not.
- **Reconnect** is automatic (native `EventSource`); on reconnect it re-sends the
  same `?sub=`, so no extra handling — just rebuild `sub` when the widget set
  changes.
- **Fires on CONTENT CHANGE, not on every reconcile.** A `refresh` event is
  emitted only when the widget's *resolved rendered output* actually changes —
  not on every Kubernetes reconcile of the underlying resource. A resource that
  reconciles repeatedly but produces **stable output** (e.g. a stuck or erroring
  composition whose status keeps re-writing to the same values) will **never**
  publish a refresh. This is by design (it avoids spurious refetches), but it
  confounds a naive test: *"I watched a failing composition and got nothing"* is
  expected, not a bug. **When testing, pick a target whose rendered content
  actually changes** (e.g. scale a deployment, edit a value the widget displays)
  — and use `/debug/refreshes` (`published` climbing) to confirm the server is
  emitting at all.

---

## 6. Config knobs (ops / snowplow side, for reference)

| Env | Default | Meaning |
|---|---|---|
| `REFRESH_SSE_ENABLED` | on (when cache on) | master toggle for the layer |
| `REFRESH_SESSION_COOKIE` | `krateo-session` | cookie name the JWT is read from |
| `REFRESH_COALESCE_WINDOW_MS` | `250` | server-side per-key dedup window |
| `REFRESH_EVICTION_PUBLISH_RATE_PER_SECOND` | `5` (provisional) | per-subscriber release rate of eviction-driven frames (§10); excess deferred, never dropped |
| `REFRESH_EVICTION_PUBLISH_BURST` | `10` (provisional) | per-subscriber burst released before the rate applies (§10) |

Live-refresh requires `CACHE_ENABLED=true` (it rides the cache's refresher).

---

## 7. Quick smoke test (non-browser)

```sh
SUB=$(printf '[{"class":"restactions","group":"templates.krateo.io","version":"v1",
  "resource":"restactions","namespace":"demo","name":"blueprints-list","extras":{}}]' \
  | base64 | tr '+/' '-_' | tr -d '=')

curl -N -H "Authorization: Bearer $JWT" \
  "https://<snowplow>/refreshes?sub=$SUB"
# → ': keepalive' every 20s; 'event: refresh / data: <l1Key>' when that RA's data changes.
```

---

## 8. Resolved items

- ✅ **`widgets` vs `widgetContent` class** ambiguity (§2.5) — **RESOLVED**.
  snowplow now stamps `X-Snowplow-Refresh-Class` on every cache-keyed `/call`
  response with the exact class it keyed under (`widgets` for RBAC-sensitive
  widgets, `widgetContent` for the shared shell, `restactions` for RESTActions).
  **Arm the subscription with that header value verbatim** — no arm-both, no
  guessing. (snowplow 1.5.5+ / the `X-Snowplow-Refresh-Class` header is in the
  CORS `ExposedHeaders` list.)

---

## 9. Recovery after an interruption — the CLIENT owns it

**Snowplow offers no replay, deliberately.** The handler sends no `id:` field, keeps no ring, and
honours no `Last-Event-ID`. A frame published while a browser was disconnected is gone. This is a
design choice, not a gap: the frames are idempotent key pings, so N missed frames for key K are
equivalent to one refetch of K — and "refetch what I am displaying" is a strictly smaller, always-
correct recovery than replaying a delta stream that may have rolled past, reset on a pod restart,
or been armed against a different widget set.

The consequence for the SPA is a contract, not an optimisation:

> After a stream drops and reconnects, the client MUST re-fetch every armed widget once. Nothing
> else will tell it what changed while it was away.

Three properties that recovery must have, and why each is load-bearing:

1. **Gate on CAUSE, not on "is this the first connect".** Arming and disarming abort and re-open
   the stream — every widget mount does. Re-validating on that path fires the whole armed set on
   every page navigation: the same amplification this section exists to bound, triggered by
   routine use instead of by a fault. Re-validate only when the stream dropped for a transport
   reason or a server idle-close.
2. **Bound concurrency and arrival rate with ONE queue.** Re-validation and frame-driven refetches
   must enqueue onto the same bounded queue (6 in flight per tab). Two separate caps do not bound
   their sum, and a stagger placed *beside* a cap only spreads an unbounded burst. Spread the
   re-validation over `W = min(30 s, N × 300 ms)`: the cap bounds how many run at once, the spread
   bounds how fast they arrive, and a fleet reconnecting together needs both.
3. **Jitter the reconnect backoff (±25 %).** Without it, tabs that dropped together retry in
   lockstep forever, turning one outage into a synchronised herd on every subsequent attempt.

**A 401 is not a transport error.** Retrying it re-presents the same dead token until the backoff
ceiling, forever, with no path back to a live stream — measured as 553 `reason=JwtAuth` rejections
at the gateway in one 4½-day window. The stream must raise the app's session-resume flow instead
and stop; re-authentication re-arms the widgets, which re-opens the stream with a fresh token.

---

## 10. Eviction now publishes a frame — and what a 404 means afterwards

From snowplow 1.12.6, a **DELETE-semantics eviction publishes a paced `refresh` frame** (token
bucket per subscriber). Two things follow for the browser, and both change existing behaviour.

**The induced refetch is a cold MISS by construction.** The entry was just evicted, so the `/call`
it triggers cannot be served from L1 and goes to the resolver. That is why the server paces
publication, and why the client must cap its own in-flight refetches: an unbounded fan-out of cold
misses is the failure mode, not the refetch itself.

**A 404 answering a frame-triggered refetch is a CONFIRMED DELETE.** This inverts the normal
policy, and only for that case. A 404 is otherwise transient — right after page load snowplow can
404 a widget whose CR exists while its informer is still cold, and retrying is what stops an error
flash on first paint. But when the refetch was triggered by a frame, snowplow has just said this
object changed, and 404 is the answer. Retrying cannot discover anything new: at 3 retries with
700/1400/2800 ms backoff it costs **four requests and ~4.9 s per widget**, a fourfold amplification
of exactly the bulk delete the server is pacing.

So the client must carry one bit — *was this refetch triggered by a frame?* — from its stream
handler into its retry policy, and treat 404 as terminal when it is set. Keep that bit as narrow as
possible: a single boolean keyed by widget id is enough, and it lets the retry policy stay ignorant
of the transport. Failing fast also shows the not-found state in ~0 s instead of ~4.9 s.

**TTL, LRU and max-age evictions stay silent, and clients must not expect frames from them.**
Those are capacity or freshness decisions about snowplow's cache, not facts about the data: the
object still exists and the body the browser holds is still valid. The same holds for the
non-404 drop-point eviction behind the refresh breaker (new in 1.12.6 Track B), which drops an
entry without any evidence the object is gone. Only the two DELETE-semantics sites publish; that third site (`EvictDropPoint`) is pinned silent by snowplow's S1d drop-point test.

---

## 11. Gateway configuration hazards — what must never be attached to this route

The stream survives the gateway today: no route policy buffers, times out, or cuts the hop at any
duration. That is **one config edit away from being false**, and the damage is invisible without
delivery metrics — every stream simply stops, and the browser degrades to looking merely stale.

None of the following may be added to `agentgateway-policies-platform-snowplow`. This belongs in
the chart review checklist as well as here, because the edit that would cause it lives in the
agentgateway-policies chart, not in snowplow or the SPA.

| never add | why it breaks the stream |
|---|---|
| `traffic.timeouts.request` | Per the CRD it covers "the time from when the request first starts being sent from the gateway to when the **full response has been received**". An SSE response never completes, so **any** value hard-cuts **every** stream at exactly that duration. It is also the only timeout this API exposes — there is no idle, stream-idle or max-duration knob — so a well-meaning "add a reasonable timeout" edit has **no safe form here**. |
| `traffic.buffer` (either direction) | Accumulates the unbounded SSE body until completion. The browser receives nothing, then gets a 502 at the 2 Mi FailClosed default. |
| `traffic.retry` | Meaningless on a streamed response, and per the CRD retry requires buffering the request body for replay. |
| `rateLimit` on this route | Not a substitute for the per-subscriber token bucket: it sheds `/call` and `/refreshes` alike at the edge, turning a paced notification into a user-facing error. |
