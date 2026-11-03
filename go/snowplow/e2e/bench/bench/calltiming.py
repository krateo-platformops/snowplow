"""#1089: how the browser fetched each /call — REPORT ONLY.

Run 20 inferred that /dashboard first visits queued for a browser connection:
the bench's origin is plain http://<gateway>:80, so Chrome speaks HTTP/1.1
with 6 connections per host, 2 of them held by the event streams. browser.js
(k6/pageload.js timingOf / callTiming) keeps, per /call Resource Timing
entry, the negotiated protocol (tag call_protocol) and its fetchStart,
connectStart, connectEnd, requestStart, responseStart, responseEnd relative
to its startTime, its start relative to its load's, and its load's id
(call_timing metadata; never a URL). Per route and visit this reports:

  protocols   the call_protocol set ('' = the browser did not expose it)
  queue       requestStart − fetchStart on a REUSED connection
              (connectStart === connectEnd, both exposed): the time the
              request waited for a free connection. Per call (its p50 / p95,
              and as a share of the call's own responseStart − startTime,
              which contains it) and per load (the sum and the longest wait
              of its calls, against the load's wall time; the calls overlap,
              so the sum can exceed the wall time)
  chrome_vs_resource_timing
              per load: Chrome's own network time (its Fetch requests' CDP
              durations) against Resource Timing's (its /calls' responseStart −
              startTime). A RESIDUAL, unexplained here: connection queueing lies
              inside BOTH sides (Chrome's socket-pool wait falls between
              requestWillBeSent and the response), so the residual cannot be
              queueing; and Chrome's side counts every Fetch of the load,
              /call or not (run 20b: 84,494 Fetches against 81,302 /calls)
  waterfall   per load: the first call's start, the last call's end and the
              load's wall time; per /call, its start from its load's start,
              its queueing and its end, in median-start order (a call that
              waits on another sorts after it)

The join (CHROME_JOIN): k6 v2.3.0 stamps a browser request sample with the
VU's metadata when its RESPONSE is emitted (AsyncMetricContext off), so a
Chrome sample carries only its VU (metadata `vu`); it belongs to the load of
that VU whose window [t0, t0 + wall] holds the request's own start (the
sample's time minus its duration). A load's id is on its own page-load point
and on its calls' call_timing.

Only a load's own calls (during=load) enter queue / residual / waterfall. A
field the browser did not expose is not a number here; a section with no
measured call reads UNMEASURED, never 0.
"""

from __future__ import annotations

import bisect
import json
from collections import defaultdict

UNMEASURED = "UNMEASURED"
CALL_KEY = ("call_resource", "call_namespace", "call_name")
CHROME_JOIN = ("a Chrome request sample (browser_http_req_duration, resource_type=Fetch) carries only its VU "
               "(metadata vu: k6 v2.3.0 stamps metadata when the response is emitted, AsyncMetricContext off); it "
               "belongs to that VU's load whose window [t0, t0 + wall] holds the request's start (sample time "
               "minus duration)")
RESIDUAL_HOW = ("chrome_ms = the sum of the CDP durations (requestWillBeSent to the response's headers) of the "
                "Fetch requests the load's VU started inside the load's window, /call or not; rt_ms = the sum of "
                "the load's /calls' responseStart - startTime; residual_ms = chrome_ms - rt_ms. Connection queueing "
                "(requestStart - fetchStart) lies inside both sides, so the residual is NOT queueing: it is "
                "unexplained here (non-/call fetches, CDP event timing)")


def _pct(vals: list[float], p: float) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    k = (len(s) - 1) * p / 100
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return round(s[lo] + (s[hi] - s[lo]) * (k - lo), 3)


def _dist(vals: list[float], unit: str = "ms") -> dict | str:
    if not vals:
        return UNMEASURED
    out = {"n": len(vals), f"p50_{unit}": _pct(vals, 50), f"p95_{unit}": _pct(vals, 95),
           f"max_{unit}": round(max(vals), 3)}
    if unit == "ms":
        out["sum_ms"] = round(sum(vals), 1)
    return out


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
    """(value, tags, metadata, time) per point; metadata / times are
    index-aligned with the points (k6eval.read_k6_out)."""
    pts = (o.get("points") or {}).get(metric) or []
    meta = list((o.get("metadata") or {}).get(metric) or [])
    meta += [{}] * (len(pts) - len(meta))
    times = list((o.get("times") or {}).get(metric) or [])
    times += [None] * (len(pts) - len(times))
    return [(v, t, md or {}, ts) for (v, t), md, ts in zip(pts, meta, times)]


def _loads(o: dict) -> dict:
    """load_id -> (vu, t0 epoch ms, wall ms) from the page-load points."""
    out = {}
    for v, t, md, _ in _aligned(o, "browser_page_load_ms"):
        lid = md.get("load_id")
        try:
            t0 = float(t.get("t0"))
        except (TypeError, ValueError):
            continue
        if lid and _num(v) is not None:
            out[str(lid)] = (str(lid).split(":", 1)[0], t0, float(v))
    return out


def _chrome_by_load(o: dict, loads: dict) -> tuple[dict, int]:
    """load_id -> [CDP ms] of the Fetch requests its VU started in its window
    (CHROME_JOIN); and how many Fetch samples carried a VU at all."""
    by_vu: dict[str, list[tuple[float, float]]] = defaultdict(list)
    seen = 0
    for v, t, md, ts in _aligned(o, "browser_http_req_duration"):
        if t.get("resource_type") != "Fetch" or not md.get("vu") or _num(v) is None or ts is None:
            continue
        seen += 1
        by_vu[str(md["vu"])].append((ts * 1000.0 - v, v))
    for xs in by_vu.values():
        xs.sort()
    out: dict[str, list[float]] = {}
    for lid, (vu, t0, wall) in loads.items():
        xs = by_vu.get(vu) or []
        i = bisect.bisect_left(xs, (t0, float("-inf")))
        j = bisect.bisect_right(xs, (t0 + wall, float("inf")))
        out[lid] = [d for _, d in xs[i:j]]
    return out, seen


def report(browser: dict | None) -> dict:
    """verdict.json's call_timing_report_only, from the browser pod's stream."""
    if browser is None:
        return {"status": UNMEASURED, "why": "no browser stream (not the k6 driver, or not collected)"}
    calls = _aligned(browser, "browser_call_ms")
    if not calls:
        return {"status": UNMEASURED, "why": "no browser_call_ms point: no /call was recorded"}
    tagged = sum(1 for _, t, _, _ in calls if "call_protocol" in t)
    timed = sum(1 for _, _, md, _ in calls if timing_of(md) is not None)
    if not tagged and not timed:
        return {"status": UNMEASURED, "calls": len(calls),
                "why": "no browser_call_ms point carries call_protocol or call_timing (a browser.js before #1089)"}
    loads = _loads(browser)
    chrome, chrome_seen = _chrome_by_load(browser, loads)
    protocols: dict[str, int] = defaultdict(int)
    for _, t, _, _ in calls:
        protocols[t.get("call_protocol", UNMEASURED)] += 1
    groups: dict[tuple, dict] = defaultdict(lambda: {"calls": 0, "protocols": defaultdict(int), "timed": 0,
                                                     "reused": 0, "new_connection": 0, "unexposed": 0,
                                                     "queue": [], "queue_share": [], "loads": defaultdict(list)})
    for v, t, md, _ in calls:
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
            rs = _num(m.get("response_start"))
            if rs:
                g["queue_share"].append(q / rs)
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
            "queue_share_of_call": _dist(g["queue_share"], unit="share"),
            "queue_per_load": _per_load(g["loads"], loads),
            "chrome_vs_resource_timing": _residual(g["loads"], chrome),
            "waterfall": _waterfall(g["loads"], loads)}
    return {"status": "present", "calls": len(calls), "calls_with_protocol": tagged, "calls_with_timing": timed,
            "protocols": dict(protocols), "loads_with_an_id": len(loads), "chrome_fetches_with_a_vu": chrome_seen,
            "by_route_visit": by, "chrome_join": CHROME_JOIN, "residual_how": RESIDUAL_HOW,
            "note": "report only; a load's own calls (during=load) only in by_route_visit; queue_ms is "
                    "requestStart - fetchStart where connectStart === connectEnd (a reused connection)"}


def _per_load(cl: dict, loads: dict) -> dict | str:
    """Per load: its calls' summed and longest queueing, against its wall time."""
    rows = []
    for lid, cs in cl.items():
        qs = [q for _, _, q in cs if q is not None]
        if not qs or lid not in loads:
            continue
        rows.append((sum(qs), max(qs), loads[lid][2]))
    if not rows:
        return {"status": UNMEASURED, "why": "no load has both a page-load point and a reused-connection call "
                                             "under one load_id"}
    return {"loads": len(rows), "queue_sum_ms": _dist([s for s, _, _ in rows]),
            "queue_max_ms": _dist([m for _, m, _ in rows]), "wall_ms": _dist([w for _, _, w in rows]),
            "queue_sum_over_wall": _dist([s / w for s, _, w in rows if w > 0], unit="share"),
            "queue_max_over_wall": _dist([m / w for _, m, w in rows if w > 0], unit="share"),
            "note": "the calls overlap: their summed waiting can exceed the wall time; queue_max is the one "
                    "longest wait of the load"}


def _residual(cl: dict, chrome: dict) -> dict:
    """Chrome's network time against Resource Timing's, per load: a residual
    (RESIDUAL_HOW), never attributed to queueing."""
    rows = []
    for lid, cs in cl.items():
        ch = chrome.get(lid)
        rt = [m["response_start"] for m, _, _ in cs if _num(m.get("response_start")) is not None]
        if not ch or not rt:
            continue
        rows.append((sum(ch), len(ch), sum(rt), len(rt)))
    if not rows:
        return {"status": UNMEASURED, "why": "no load has both Chrome Fetch samples in its window and its calls' "
                                             "responseStart (no `vu` metadata: a browser.js before #1089)"}
    ch, chn, rt, rtn = (sum(r[i] for r in rows) for i in range(4))
    return {"loads": len(rows), "chrome_ms": round(ch, 1), "chrome_fetches": chn, "rt_ms": round(rt, 1),
            "rt_calls": rtn, "residual_ms": round(ch - rt, 1),
            "residual_ms_per_load": _dist([c - r for c, _, r, _ in rows]),
            "is_queueing": False, "why": "queueing lies inside both sides; see residual_how"}


def _waterfall(cl: dict, loads: dict) -> dict | str:
    """Per load: first call start, last call end, wall time; per /call
    (resource / namespace / name), over the loads: its start from its load's
    start, its queueing and its end, in median-start order."""
    if not cl:
        return UNMEASURED
    first, last, walls = [], [], []
    per: dict[tuple, dict] = defaultdict(lambda: {"start": [], "queue": [], "end": [], "protocols": set()})
    for lid, cs in cl.items():
        first.append(min(m["from_load_ms"] for m, _, _ in cs))
        ends = [m["from_load_ms"] + m["response_end"] for m, _, _ in cs if _num(m.get("response_end")) is not None]
        if ends:
            last.append(max(ends))
        if lid in loads:
            walls.append(loads[lid][2])
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
    return {"loads": len(cl), "first_call_start_ms": _dist(first), "last_call_end_ms": _dist(last),
            "load_wall_ms": _dist(walls),
            "calls": sorted(rows, key=lambda r: (r["start_p50_ms"], r["call"]))}
