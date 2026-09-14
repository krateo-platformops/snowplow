#!/usr/bin/env python3
"""Generate the HyperDX v2 dashboard JSON for the snowplow SLIs (1.12.4 design §9 shape, rebuilt from the live metric inventory).
Usage: hdx_dashboard.py [--trial] > dashboard.json
"""
import json, sys

METRICS = "6a8c6a42b001b331cb401a72"
LOGS = "6a8c6a42b001b331cb401a6e"
SVC = "ServiceName = 'snowplow'"

tiles = []
def tile(name, x, y, w, h, cfg):
    cfg.setdefault("sourceId", METRICS)
    cfg.setdefault("whereLanguage", "sql")
    tiles.append({"x": x, "y": y, "w": w, "h": h, "name": name, "config": cfg})

def sel(metric, mtype, agg="max", where="", alias=None, value="Value"):
    # The tile-level `where` is dropped by the API schema (trial 1), so the service filter lives in every select.
    w = SVC if not where else f"{SVC} AND {where}"
    if agg == "count":
        value = ""  # the API rejects a value expression with count
    s = {"aggFn": agg, "valueExpression": value, "metricName": metric, "metricType": mtype,
         "where": w, "whereLanguage": "sql"}
    if alias: s["alias"] = alias
    return s

def stat(metric, mtype, name, agg="max", alias=None):
    return sel(metric, mtype, agg, f"Attributes['stat'] = '{name}'", alias or name)

W, H = 8, 4
# Row 0 — cache health
tile("L1 lookups by outcome (hit / miss / seed_hit)", 0, 0, W, H, {"displayType": "line", "where": SVC,
     "select": [sel("snowplow_dispatch_l1_lookups_total", "sum", "sum")], "groupBy": "Attributes['outcome']"})
tile("Resolved cache: entries / bytes", 8, 0, W, H, {"displayType": "line", "where": SVC,
     "select": [stat("snowplow_resolved_cache", "gauge", "entries"), stat("snowplow_resolved_cache", "gauge", "bytes")]})
tile("Resolved cache evictions (ttl / lru / delete)", 16, 0, W, H, {"displayType": "line", "where": SVC,
     "select": [stat("snowplow_resolved_cache", "gauge", "evict_ttl_total", alias="ttl"),
                stat("snowplow_resolved_cache", "gauge", "evict_lru_total", alias="lru"),
                stat("snowplow_resolved_cache", "gauge", "evict_delete_total", alias="delete")]})
# Row 1 — fallthrough SLI (1.12.4 reclassification: expect diagnostic ≫ fallthrough, ~8:1)
tile("Apiserver fall-through (genuine live reads)", 0, 4, W, H, {"displayType": "line", "where": SVC,
     "select": [sel("snowplow_apiserver_fallthrough_total", "sum", "sum", alias="fallthrough")]})
tile("Cache diagnostic hits (NOT apiserver reads)", 8, 4, W, H, {"displayType": "line", "where": SVC,
     "select": [sel("snowplow_cache_diagnostic_total", "sum", "sum", alias="diagnostic")]})
tile("Fall-through cells by reason", 16, 4, W, H, {"displayType": "line", "where": SVC,
     "select": [sel("snowplow_apiserver_fallthrough_cells_total", "sum", "sum")], "groupBy": "Attributes['reason']"})
# Row 2 — UAF declines (1.12.3): non-zero is CORRECT until 1.13.0 folds UAF scope into the key
tile("UAF Put declines (non-zero is correct in 1.12.x; returns to 0 in 1.13.0)", 0, 8, W, H, {"displayType": "line", "where": SVC,
     "select": [sel("snowplow_restactions_uaf_put_declined_total", "sum", "sum", alias="restactions"),
                sel("snowplow_widgets_uaf_put_declined_total", "sum", "sum", alias="widgets"),
                sel("snowplow_ra_full_list_uaf_bypass_total", "sum", "sum", alias="raFullList bypass")]})
tile("Authz memo hits / misses / deny-uncached", 8, 8, W, H, {"displayType": "line", "where": SVC,
     "select": [sel("snowplow_authz_memo_hits", "sum", "sum", alias="hits"),
                sel("snowplow_authz_memo_misses", "sum", "sum", alias="misses"),
                sel("snowplow_authz_memo_deny_uncached_total", "sum", "sum", alias="deny_uncached")]})
tile("Refresher outcomes", 16, 8, W, H, {"displayType": "line", "where": SVC,
     "select": [sel("snowplow_refresher", "sum", "sum")], "groupBy": "Attributes['stat']"})
# Row 3 — #187 containment (1.12.5): the primary path works when evict_delete moves and the two fallbacks stay 0
tile("Dep tracker: evictions by route (delete / self-gone / self-notfound)", 0, 12, W, H, {"displayType": "line", "where": SVC,
     "select": [stat("snowplow_deps", "gauge", "evict_delete_total", alias="informer DELETE"),
                stat("snowplow_deps", "gauge", "evict_self_gone_total", alias="self gone"),
                stat("snowplow_deps", "gauge", "self_notfound_evict_total", alias="self 404 (drop point)")]})
tile("Dep tracker health: worker panics / queue full / cap drops (alert on > 0)", 8, 12, W, H, {"displayType": "line", "where": SVC,
     "select": [stat("snowplow_deps", "gauge", "delete_worker_panics_total", alias="worker panics"),
                stat("snowplow_deps", "gauge", "delete_queue_full_total", alias="queue full"),
                stat("snowplow_deps", "gauge", "dropped_cap", alias="cap drops")]})
tile("Dirty-marks / dep records", 16, 12, W, H, {"displayType": "line", "where": SVC,
     "select": [stat("snowplow_deps", "gauge", "dirty_mark_total", alias="dirty marks"),
                stat("snowplow_deps", "gauge", "records", alias="dep records")]})
# Row 4 — informers
tile("Informers: registered / synced / servable / watch_broken", 0, 16, W, H, {"displayType": "line", "where": SVC,
     "select": [sel("snowplow_informer_servable", "gauge", "max")], "groupBy": "Attributes['state']"})
tile("Informer freshness: max event age (s) / GVRs stale > 1h", 8, 16, W, H, {"displayType": "line", "where": SVC,
     "select": [stat("snowplow_informer_freshness", "gauge", "max_event_age_seconds", alias="max event age s"),
                stat("snowplow_informer_freshness", "gauge", "gvrs_stale_over_hour", alias="gvrs stale >1h")]})
tile("Refresher queue depth / prewarm engine pending", 16, 16, W, H, {"displayType": "line", "where": SVC,
     "select": [sel("snowplow_refresher_queue_depth", "gauge", "max", alias="refresher queue"),
                sel("snowplow_prewarm_engine_pending_depth", "gauge", "max", alias="prewarm pending")]})
# Row 5 — HTTP (otelhttp, the free per-route latency SLI) + logs + build
tile("HTTP server request duration p95 by route (s)", 0, 20, W, H, {"displayType": "line", "where": SVC,
     "select": [sel("http.server.request.duration", "histogram", "quantile", value="Value", alias="p95")],
     "groupBy": "Attributes['http.route']"})
tile("HTTP requests by status code", 8, 20, W, H, {"displayType": "line", "where": SVC,
     "select": [sel("http.server.request.duration", "histogram", "count", alias="requests")],
     "groupBy": "Attributes['http.response.status_code']"})
tile("snowplow ERROR / WARN log lines (Body match — SeverityText is empty until observability#54)", 16, 20, W, H,
     {"displayType": "line", "sourceId": LOGS, "where": SVC,
      "select": [{"aggFn": "count", "valueExpression": "", "where": SVC + " AND positionCaseInsensitive(Body, '\"level\":\"ERROR\"') > 0", "whereLanguage": "sql", "alias": "ERROR"},
                 {"aggFn": "count", "valueExpression": "", "where": SVC + " AND positionCaseInsensitive(Body, '\"level\":\"WARN\"') > 0", "whereLanguage": "sql", "alias": "WARN"}]})

# Fix the p95 select: HyperDX quantile aggFn needs the level
for t in tiles:
    for s in t["config"]["select"]:
        if s.get("aggFn") == "quantile":
            s["level"] = 0.95

dash = {"name": "Snowplow / Cache & Prewarm SLIs", "tiles": tiles, "tags": ["snowplow", "1.12.4", "1.12.5"]}
if "--trial" in sys.argv:
    dash = {"name": "snowplow-dashboard-trial", "tiles": [tiles[1], tiles[15], tiles[17]], "tags": ["trial"]}
print(json.dumps(dash))
