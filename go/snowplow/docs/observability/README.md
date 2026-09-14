# HyperDX dashboard — "Snowplow / Cache & Prewarm SLIs" (dashboards as code)

`hyperdx_snowplow_slis.py` renders the dashboard JSON; `hyperdx-snowplow-slis.json` is the rendered form
that was POSTed to the HyperDX v2 API on krateo-057 on 2026-09-14 (dashboard id `6aa84a00b001b331cb4a0574`).

## Re-create / update
```bash
kubectl --context=<ctx> -n krateo-system port-forward svc/krateo-clickstack-api 8000:8000 &
TOK=$(kubectl --context=<ctx> -n krateo-system get secret hyperdx-api-token -o json | jq -r '.data.accessKey' | base64 -d)
python3 hyperdx_snowplow_slis.py > /tmp/dash.json
curl -s -X POST -H "Authorization: Bearer $TOK" -H 'Content-Type: application/json' --data @/tmp/dash.json http://127.0.0.1:8000/api/v2/dashboards
```
The `accessKey` field is the API key (the `token` field is not). Source ids are cluster-specific: read them from
`GET /api/v2/sources` (Metrics / Logs / Traces) and set them at the top of the generator.

## Schema notes (learned against HyperDX 2.35.0)
- A tile-level `where` is silently dropped; every `select` carries its own `where` (this is why `ServiceName = 'snowplow'` appears in each one).
- `aggFn: count` rejects a `valueExpression` (must be `""`).
- `alias`, `groupBy` and `level` (for `quantile`) persist.
- Log-line panels filter on `Body` (`positionCaseInsensitive(Body, '"level":"ERROR"')`) because the platform's filelog pipeline sets no `SeverityText` (krateo-platformops/observability#54).

## Panels (18)
Cache health (L1 lookups by outcome; resolved-cache entries/bytes; evictions ttl/lru/delete) · fall-through SLI (genuine apiserver reads vs cache-diagnostic hits, cells by reason — expect ~1:8 after the 1.12.4 reclassification) · UAF Put declines (non-zero is correct in 1.12.x, returns to 0 in 1.13.0) · authz memo · refresher outcomes · #187 containment (dep-tracker evictions by route; worker panics / queue full / cap drops — alert on > 0; dirty-marks / records) · informers (servable states; freshness: max event age, GVRs stale > 1 h) · refresher queue / prewarm pending · HTTP server duration p95 by route and requests by status code (otelhttp) · ERROR/WARN log lines.
