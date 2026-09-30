# POST /call/read — the extras body channel (#186)

## Why
A `/call` read supplies its optional `extras` JSON via `?extras=<json>` in the URL. Over HTTP/2 that rides the `:path` pseudo-header, charged to the gateway's request-header budget (~16 KiB Envoy / ~8 KB nginx) alongside the Bearer JWT and cookies. A large `extras` blows the budget → the gateway returns **431** before snowplow is ever invoked (snowplow's own 1 MiB header limit is never reached).

**Concrete victim (grounded):** the Autopilot **previewBlueprint** flow. `ui/src/components/Autopilot/previewBridge.ts` → `callRenderRestAction` (:277-294) sets `url.searchParams.set('extras', extras)` where `extras` = `buildBlueprintRenderExtras(args)` (:223) — the **whole chart** (Chart.yaml + values.yaml, or rawTemplates). Its own comment at :292-293 already names "the gateway's 431 for a URL past its header limit." So autopilot preview is dead on every proxied install.

## The endpoint contract
`POST /call/read` — a body-carrying READ. Identical semantics to `GET /call` except `extras` moves from the query to the body.
- **Addressing STAYS in the query:** `resource`, `apiVersion`, `name`, `namespace`, `page`, `perPage` — exactly as `GET /call`. Only `extras` moves.
- **Body:** `{"extras": { ... }}` — a wrapper object (forward-compatible; a future read-body field can be added without ambiguity). The inner `extras` value is byte-for-byte what `?extras=<same json>` carries, so the two channels derive the **identical cache key** (parity by construction — same decoded map → same hash → same L1 cell → a HIT).
- **Read-only guarantee (security):** the route can NEVER create or mutate. The apiserver verb is forced to GET and the body is never forwarded as an object payload — even on a dispatch miss (an unhandled GVR hits a read-only fallthrough, not the write path). A `POST /call/read` to a writable GVR performs a READ, never a create. (Contrast `POST /call`, which is the write path.)
- **Body cap:** 1 MiB (a fixed DoS ceiling, like the header limit — not tunable). A malformed body or one over the cap → **400**, never forwarded.
- **`GET /call?extras=` is UNCHANGED** — existing clients keep working byte-identically.

## SPA integration
**The one change:** `previewBridge.ts` `callRenderRestAction` (:277-294) switches to the body channel:
- Keep the query addressing (resource/apiVersion/name/namespace).
- Send `{"extras": <the-blueprint-render-extras-object>}` as the POST body instead of `?extras=`.
- `fetch(url, { method: 'POST', body, headers: { 'Content-Type':'application/json', ...authHeader() } })`.

This covers both RAs that share `callRenderRestAction`: `blueprint-render` (chart in extras) and `blueprint-render-draft` (:272-274).

**Which calls switch (the threshold, grounded per-call-site):**
- **Body channel:** the LARGE-extras callers — previewBlueprint's chart/template content (`callRenderRestAction`). This is the only SPA site whose `extras` carries whole-file content, and the only one that 431s.
- **Keep the query:** the SMALL-extras callers — CommandPalette (`?extras={"q":<term>}`, useSearchTypeahead.ts:94), PageSearch (`buildExtrasParam`), navigation (`?projects=`), identity extras (ConfigContext), and upgrade-impact (`buildUpgradeImpactExtras` — composition identity + target version, previewBridge.ts:684). These are bounded (a search term, a project list, an identity tuple) and never approach the header budget; leave them unchanged.
- **General guard (robust option):** if you prefer a size-based switch over per-call-site, gate on the serialized `extras` length — e.g. `> ~8 KiB → body channel` — at the shared `/call` request builder, so any future large-extras caller is covered automatically. Either is acceptable; per-call-site is simpler and matches today's structure (previewBridge is the sole large-extras caller).

## Versioning / rollout (independent repos)
The SPA and snowplow version independently. The SPA sends **POST /call/read** and falls back to **GET /call?extras=** on a **404** (an older snowplow without the route). So:
- New SPA + new snowplow → body channel (no 431).
- New SPA + old snowplow → 404 on /call/read → GET fallback (may 431 on a huge chart, same as today — no regression).
- Old SPA + new snowplow → unchanged GET ?extras= (still works).

No coordinated deploy required.

## Non-goals
- `/call/read` is a per-user READ, not a client-coordination channel — it fetches resolved data, it does not let clients signal each other.
- Write verbs (`POST/PUT/PATCH/DELETE /call`) are unchanged.

---
_Authored by arch-1217; SPA references (previewBridge.ts `callRenderRestAction`:277, `?extras` at :289, GET fetch at :290, the 431 comment at :292-293, `buildBlueprintRenderExtras`:223, `blueprint-render` at :312, `upgrade-impact` at :684) verified in `~/krateo/frontend-draganddrop/frontend/ui/src/components/Autopilot/previewBridge.ts` as part of #186. Backend wire contract locked by snowplow PR #340._
