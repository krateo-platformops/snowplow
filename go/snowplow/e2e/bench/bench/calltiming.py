"""#1089: how the browser fetched each /call — REPORT ONLY.

Run 20 inferred that /dashboard first visits queued for a browser connection:
the bench's origin is plain http://<gateway>:80, so Chrome speaks HTTP/1.1
with 6 connections per host, 2 of them held by the event streams. browser.js
(k6/pageload.js timingOf / callTiming) now keeps, per /call Resource Timing
entry, the negotiated protocol (tag call_protocol) and its fetchStart,
connectStart, connectEnd, requestStart, responseStart, responseEnd relative
to its startTime, its start relative to its load's, and its load's id
(call_timing metadata; never a URL). The same load id rides on the VU's
metadata while the load runs, so Chrome's own request samples of that load
(browser_http_req_duration: CDP requestWillBeSent to the response's headers)
and the load's page-load point carry it. Per route and visit this reports:

  protocols   the call_protocol set ('' = the browser did not expose it)
  queue_ms    requestStart − fetchStart on a REUSED connection
              (connectStart === connectEnd: no connect of its own) — the time
              the request waited for a free connection
  chrome_gap  per load: Chrome's network time (its Fetch requests' CDP
              durations) minus Resource Timing's (its /calls' responseStart −
              startTime), and the load's queue_ms as a share of that gap
  waterfall   per load: the first call's start, the last call's end and the
              load's wall time; per /call, its start from its load's start,
              its queueing and its end, in median-start order (a call that
              waits on another sorts after it)

Only a load's own calls (during=load) enter queue / gap / waterfall. A field
the browser did not expose is not a number here; a section with no measured
call reads UNMEASURED, never 0.
"""

from __future__ import annotations

import json
from collections import defaultdict

UNMEASURED = "UNMEASURED"
CALL_KEY = ("call_resource", "call_namespace", "call_name")
GAP_HOW = ("per load (joined by its load_id, never by URL): chrome_ms = the sum of Chrome's network time of the "
           "Fetch requests sent during the load (browser_http_req_duration, resource_type=Fetch: CDP "
           "requestWillBeSent to the response's headers; every fetch of the load, /call or not); rt_ms = the sum "
           "of the load's /calls' Resource Timing to the response's headers (responseStart - startTime); gap_ms "
           "= chrome_ms - rt_ms; queue_share = the load's summed queue_ms / gap_ms, over the route's loads with "
           "both sides recorded (a gap <= 0 has no share: said, never 0)")


def _pct(vals: list[float], p: float) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    k = (len(s) - 1) * p / 100
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return round(s[lo] + (s[hi] - s[lo]) * (k - lo), 1)


def _dist(vals: list[float]) -> dict | str:
    if not vals:
        return UNMEASURED
    return {"n": len(vals), "p50_ms": _pct(vals, 50), "p95_ms": _pct(vals, 95), "max_ms": round(max(vals), 1),
            "sum_ms": round(sum(vals), 1)}


def _num(v):
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def timing_of(meta: dict | None) -> dict | None:
    """A point's call_timing metadata parsed, or None (none recorded / malformed)."""
    raw = (meta or {}).get("call_timing")
    if not isinstance(raw, str):
        return None
    try:
        m = json.loads(raw)
    except ValueError:
        return None
    return m if isinstance(m, dict) else None


def queue_ms(m: dict) -> float | None:
    """requestStart − fetchStart when the call reused a connection
    (connectStart === connectEnd, both exposed); None otherwise."""
    cs, ce, f, rq = (_num(m.get(k)) for k in ("connect_start", "connect_end", "fetch", "request_start"))
    if cs is None or ce is None or f is None or rq is None or cs != ce:
        return None
    return rq - f


def _aligned(o: dict, metric: str) -> list[tuple]:
    pts = (o.get("points") or {}).get(metric) or []
    meta = list((o.get("metadata") or {}).get(metric) or [])
    meta += [{}] * (len(pts) - len(meta))
    return [(v, t, md or {}) for (v, t), md in zip(pts, meta)]


def report(browser: dict | None) -> dict:
    """verdict.json's call_timing_report_only, from the browser pod's stream."""
    if browser is None:
        return {"status": UNMEASURED, "why": "no browser stream (not the k6 driver, or not collected)"}
    calls = _aligned(browser, "browser_call_ms")
    if not calls:
        return {"status": UNMEASURED, "why": "no browser_call_ms point: no /call was recorded"}
    tagged = sum(1 for _, t, _ in calls if "call_protocol" in t)
    timed = sum(1 for _, _, md in calls if timing_of(md) is not None)
    if not tagged and not timed:
        return {"status": UNMEASURED, "calls": len(calls),
                "why": "no browser_call_ms point carries call_protocol or call_timing (a browser.js before #1089)"}
    # Chrome's own network time per load, and each load's wall time, by load_id
    chrome: dict[str, list[float]] = defaultdict(list)
    for v, t, md in _aligned(browser, "browser_http_req_duration"):
        if md.get("load_id") and t.get("resource_type") == "Fetch" and _num(v) is not None:
            chrome[md["load_id"]].append(v)
    wall = {md["load_id"]: v for v, _, md in _aligned(browser, "browser_page_load_ms") if md.get("load_id")}
    protocols: dict[str, int] = defaultdict(int)
    for _, t, _ in calls:
        protocols[t.get("call_protocol", UNMEASURED)] += 1
    groups: dict[tuple, dict] = defaultdict(lambda: {"calls": 0, "protocols": defaultdict(int), "timed": 0,
                                                     "reused": 0, "new_connection": 0, "unexposed": 0,
                                                     "queue": [], "loads": defaultdict(list)})
    for v, t, md in calls:
        if t.get("during") != "load":
            continue
        g = groups[(t.get("route"), t.get("visit"))]
        g["calls"] += 1
        g["protocols"][t.get("call_protocol", UNMEASURED)] += 1
        m = timing_of(md)
        if m is None:
            continue
        g["timed"] += 1
        q = queue_ms(m)
        if q is not None:
            g["reused"] += 1
            g["queue"].append(q)
        elif _num(m.get("connect_start")) is not None and _num(m.get("connect_end")) is not None:
            g["new_connection"] += 1
        else:
            g["unexposed"] += 1
        if _num(m.get("from_load_ms")) is not None and m.get("load_id"):
            g["loads"][str(m["load_id"])].append((m, t, q))
    by = {}
    for (route, visit), g in sorted(groups.items(), key=lambda kv: (str(kv[0][0]), str(kv[0][1]))):
        by[f"{route} {visit}"] = {
            "route": route, "visit": visit, "calls": g["calls"], "protocols": dict(g["protocols"]),
            "timing_recorded": g["timed"], "reused_connection": g["reused"], "new_connection": g["new_connection"],
            "connection_not_exposed": g["unexposed"],
            "queue_ms": _dist(g["queue"]),
            "chrome_gap": _gap(g["loads"], chrome),
            "waterfall": _waterfall(g["loads"], wall)}
    return {"status": "present", "calls": len(calls), "calls_with_protocol": tagged, "calls_with_timing": timed,
            "protocols": dict(protocols), "loads_with_chrome_requests": len(chrome),
            "by_route_visit": by, "gap_how": GAP_HOW,
            "note": "report only; a load's own calls (during=load) only in by_route_visit; queue_ms is "
                    "requestStart - fetchStart where connectStart === connectEnd (a reused connection)"}


def _gap(loads: dict, chrome: dict) -> dict | str:
    """Chrome's network time against Resource Timing's, per load, and the
    load's queueing as a share of the difference (GAP_HOW)."""
    rows = []
    for lid, cs in loads.items():
        if lid not in chrome:
            continue
        rt = [m["response_start"] for m, _, _ in cs if _num(m.get("response_start")) is not None]
        if not rt:
            continue
        rows.append((sum(chrome[lid]), sum(rt), sum(q for _, _, q in cs if q is not None)))
    if not rows:
        return {"status": UNMEASURED, "why": "no load has both Chrome's request samples and its calls' "
                                             "responseStart under one load_id (a browser.js before #1089)"}
    ch, rt, qu = (sum(r[i] for r in rows) for i in range(3))
    gap = ch - rt
    return {"loads": len(rows), "chrome_ms": round(ch, 1), "rt_ms": round(rt, 1), "gap_ms": round(gap, 1),
            "queue_ms": round(qu, 1),
            "queue_share": round(qu / gap, 3) if gap > 0 else f"{UNMEASURED}: gap_ms <= 0 (Chrome's network time "
                                                              f"does not exceed Resource Timing's)",
            "gap_ms_per_load": _dist([c - r for c, r, _ in rows]),
            "queue_ms_per_load": _dist([q for _, _, q in rows])}


def _waterfall(loads: dict, wall: dict) -> dict | str:
    """Per load: first call start, last call end, wall time; per /call
    (resource / namespace / name), over the loads: its start from its load's
    start, its queueing and its end, in median-start order."""
    if not loads:
        return UNMEASURED
    first, last, walls = [], [], []
    per: dict[tuple, dict] = defaultdict(lambda: {"start": [], "queue": [], "end": [], "protocols": set()})
    for lid, cs in loads.items():
        first.append(min(m["from_load_ms"] for m, _, _ in cs))
        ends = [m["from_load_ms"] + m["response_end"] for m, _, _ in cs if _num(m.get("response_end")) is not None]
        if ends:
            last.append(max(ends))
        if _num(wall.get(lid)) is not None:
            walls.append(wall[lid])
        for m, t, q in cs:
            p = per[tuple(t.get(x, "") for x in CALL_KEY)]
            p["start"].append(m["from_load_ms"])
            if q is not None:
                p["queue"].append(q)
            if _num(m.get("response_end")) is not None:
                p["end"].append(m["from_load_ms"] + m["response_end"])
            p["protocols"].add(t.get("call_protocol", UNMEASURED))
    rows = [{"call": "/".join(k), "n": len(p["start"]), "start_p50_ms": _pct(p["start"], 50),
             "start_p95_ms": _pct(p["start"], 95), "queue": _dist(p["queue"]),
             "end_p50_ms": _pct(p["end"], 50), "end_p95_ms": _pct(p["end"], 95),
             "protocols": sorted(p["protocols"])} for k, p in per.items()]
    return {"loads": len(loads), "first_call_start_ms": _dist(first), "last_call_end_ms": _dist(last),
            "load_wall_ms": _dist(walls),
            "calls": sorted(rows, key=lambda r: (r["start_p50_ms"], r["call"]))}
