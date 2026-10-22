"""Read and judge run 11's k6 outputs (#830): the k6 background load and the
real-Chrome page loads.

Each pod leaves, under its collected /out:
  <name>.json.gz        k6's metric stream (--out json): one JSON object per
                        line, `{"type": "Point", "metric", "data": {value,
                        tags, time}}`, framed by bench_frame{frame:start|end}
  <name>-summary.json   handleSummary's totals (the evaluator matches the
                        stream's page-load count against it)
  <name>-exit.json      the wrapper's exit record

The k6 pod's viewer sidecar (load606.py view-feed) leaves viewer.jsonl
(viewer_start .. viewer_end) and viewer-exit.json under the same /out.

A stream that cannot prove it is whole, k6 iterations dropped beyond the bar
(an arrival-rate executor counts every start it could not make — run 10's
lag, made visible), or too few real-browser sessions make the run INVALID,
each with its reason. The north-star bars apply to the BROWSER page loads.
"""

from __future__ import annotations

import gzip
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from itertools import pairwise

__all__ = [
    "browser_checks",
    "browser_navs",
    "browser_report",
    "k6_checks",
    "k6_report",
    "read_k6_out",
    "read_viewer_out",
    "viewer_checks",
    "viewer_report",
]

# Bars (THRESHOLDS-style: written here, never derived from the instrument).
K6_THRESHOLDS = {
    # arrival-rate starts k6 could not make, as a share of the scheduled ones
    "max_dropped_share": 0.001,
    "max_k6_error_rate": 0.01,
    "min_browser_sessions": 3,
    "min_browser_loads_per_session": 20,
    # open tabs: users whose /refreshes stream opened, of the users k6 brought
    # to a working portal (k6's own first loads, not the sidecar's count)
    "min_armed_user_share": 0.9,
    # per browser session: the SPA's live-update streams that opened
    "min_sse_opened_per_stream": 1,
    # Chrome's CPU: the p95 of its 5 s windows stays under this share of its
    # request. The request is what the scheduler guarantees Chrome against
    # k6/the viewer (both now limited); a p95 under 80 % of it means Chrome
    # never needed more than its guaranteed share, so the load generator
    # could not have slowed a judged page (#831 gate M1). The 20 % margin
    # covers the 5 s window averaging away sub-window bursts.
    "max_browser_cpu_p95_share_of_request": 0.8,
    "min_cpu_windows": 3,
    # page loads that ended with a widget-error / widget-timeout state
    # (WidgetStates.tsx:34,54; forbidden / notfound are RBAC answers, not
    # failures): the same 1 % as the k6 error-rate bar. Such a page did not
    # load its content, so its time cannot stand for a loaded page.
    "max_widget_error_share": 0.01,
}

_KEEP = ("page_load_ms", "page_loads", "page_errors", "first_load_failed",
         "route_table_mismatch", "bench_frame", "http_req_failed", "http_req_duration",
         "browser_page_load_ms", "browser_page_loads", "browser_widget_states",
         "browser_page_timeouts", "browser_logins", "browser_signin_ms", "browser_sse_opened",
         "dropped_iterations")
_METRIC = re.compile(r'\{"metric":"([^"]+)"')
# the page loads whose time is kept too (out["times"], index-aligned with
# out["points"]): run 12 compares latency inside / outside the CPU profiles
_TIMED = ("page_load_ms", "browser_page_load_ms")
_RFC3339 = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)$")


def epoch(ts: str | None) -> float | None:
    """k6's point time (RFC 3339, nanoseconds) as epoch seconds."""
    m = _RFC3339.match(ts or "")
    if not m:
        return None
    tz = "+00:00" if m.group(3) == "Z" else m.group(3)
    base = datetime.fromisoformat(m.group(1) + tz).timestamp()
    return base + (float("0." + m.group(2)) if m.group(2) else 0.0)


def _pct(vals, p):
    if not vals:
        return None
    v = sorted(vals)
    k = (len(v) - 1) * p / 100.0
    lo, hi = int(k), min(int(k) + 1, len(v) - 1)
    return round(v[lo] + (v[hi] - v[lo]) * (k - lo), 1)


def read_k6_out(d: str, name: str) -> dict | None:
    """The stream, summary and exit record one k6 pod left in collected dir d
    (None when nothing of it was collected)."""
    stream = os.path.join(d, f"{name}.json.gz")
    summary = os.path.join(d, f"{name}-summary.json")
    exitf = os.path.join(d, f"{name}-exit.json")
    if not any(os.path.exists(p) for p in (stream, summary, exitf)):
        return None
    out = {"name": name, "stream": os.path.exists(stream), "points": defaultdict(list),
           "times": defaultdict(list),
           "frames": Counter(), "truncated": False, "summary": None, "exit": None}
    if out["stream"]:
        try:
            with gzip.open(stream, "rt") as fh:
                for line in fh:
                    # most lines are http_req_* timings the evaluator does not
                    # read: skip them before parsing (k6 writes "metric" first)
                    m = _METRIC.match(line)
                    if m and m.group(1) not in _KEEP:
                        continue
                    try:
                        r = json.loads(line)
                    except ValueError:
                        out["truncated"] = True          # a cut last line
                        continue
                    if r.get("type") != "Point" or r.get("metric") not in _KEEP:
                        continue
                    data = r.get("data") or {}
                    tags = data.get("tags") or {}
                    if r["metric"] == "bench_frame":
                        out["frames"][tags.get("frame")] += 1
                    else:
                        out["points"][r["metric"]].append((data.get("value"), tags))
                        if r["metric"] in _TIMED:
                            out["times"][r["metric"]].append(epoch(data.get("time")))
        except (OSError, EOFError):
            out["truncated"] = True                      # a cut gzip member
    for key, path in (("summary", summary), ("exit", exitf)):
        try:
            with open(path) as fh:
                out[key] = json.load(fh)
        except (OSError, ValueError):
            pass
    out["cpu"] = {c: read_cpu(d, c) for c in ((name, "viewer") if name == "k6" else (name,))}
    try:
        with open(os.path.join(d, f"{name}-forward.json")) as fh:
            out["forward"] = json.load(fh)   # the browser wrapper's wait for its forwards
    except (OSError, ValueError):
        out["forward"] = None
    return out


def read_cpu(d: str, container: str) -> list[dict] | None:
    """<container>-cpu.jsonl: the wrapper's cgroup samples (k6_job._cpu_sampler)."""
    try:
        with open(os.path.join(d, f"{container}-cpu.jsonl")) as fh:
            return [json.loads(ln) for ln in fh if ln.strip().startswith("{")]
    except (OSError, ValueError):
        return None


def cpu_stats(recs: list[dict] | None) -> dict | None:
    """Cores used per sample window, its p95/max, and the throttled share."""
    if not recs:
        return None
    start = next((r for r in recs if r.get("kind") == "cpu_start"), {})
    pts = [r for r in recs if r.get("kind") == "cpu" and r.get("usage_usec") is not None]
    cores, thr = [], []
    for a, b in pairwise(pts):
        dt = float(b["t"]) - float(a["t"])
        if dt <= 0:
            continue
        cores.append((b["usage_usec"] - a["usage_usec"]) / 1e6 / dt)
        if a.get("throttled_usec") is not None and b.get("throttled_usec") is not None:
            thr.append((b["throttled_usec"] - a["throttled_usec"]) / 1e6 / dt)
    return {"request_m": start.get("request_m"), "limit_m": start.get("limit_m"),
            "cgroup_limit_m": start.get("cgroup_limit_m"),
            "windows": len(cores), "p95_cores": _pct(cores, 95) if cores else None,
            "max_cores": round(max(cores), 2) if cores else None,
            "throttled_s_per_s_max": round(max(thr), 3) if thr else None}


def cpu_check(o: dict | None, container: str, th: dict | None = None) -> dict:
    """The browser container kept CPU headroom (K6_THRESHOLDS
    max_browser_cpu_p95_share_of_request)."""
    th = dict(K6_THRESHOLDS, **(th or {}))
    st = cpu_stats(((o or {}).get("cpu") or {}).get(container))
    name = f"{container}.cpu_headroom"
    share = th["max_browser_cpu_p95_share_of_request"]
    if st is None or not st["request_m"] or st["windows"] < th["min_cpu_windows"]:
        return {"check": name, "value": st, "bar": f"p95 <= {share} x request",
                "result": "FAIL", "why": f"no CPU samples for the {container} container: its "
                                         f"headroom is unknown"}
    cap = round(share * st["request_m"] / 1000, 2)
    ok = st["p95_cores"] <= cap
    return {"check": name, "value": st, "bar": f"p95 <= {cap} cores ({share} x request)",
            "result": "PASS" if ok else "FAIL",
            **({} if ok else {"why": f"the {container} container used {st['p95_cores']} cores at "
                                     f"p95, over {cap}: it may have been CPU-starved, so its "
                                     f"page timings may be the load generator's, not snowplow's"})}


def _metric(summary: dict | None, name: str, field: str):
    return (((summary or {}).get("metrics") or {}).get(name) or {}).get(field)


def _stream_check(o: dict | None, label: str, loads_metric: str) -> dict:
    """#818's framing, for a k6 pod: start and end frames, the summary's
    page-load count = the stream's, exit rc 0, no cut line."""
    if o is None:
        return {"check": f"{label}.stream_complete", "value": None, "bar": "collected",
                "result": "FAIL", "why": f"no {label} output was collected"}
    why = []
    if not o["stream"]:
        why.append("no metric stream")
    if not o["frames"].get("start"):
        why.append("no start frame: the stream's head is missing")
    if not o["frames"].get("end"):
        why.append("no end frame: k6 did not finish, or the stream's tail is missing")
    if o["truncated"]:
        why.append("the stream is cut (an unreadable line or gzip member)")
    if o["summary"] is None:
        why.append("no summary: k6 did not finish")
    else:
        want = _metric(o["summary"], loads_metric, "count") or 0
        have = len(o["points"].get(loads_metric, []))
        if want != have:
            why.append(f"the summary counts {want} page loads, the stream holds {have}")
    if o["exit"] is None:
        why.append("no exit record: the pod died before its wrapper recorded the exit code")
    elif o["exit"].get("rc") != 0:
        why.append(f"k6 exited rc={o['exit'].get('rc')}")
    return {"check": f"{label}.stream_complete",
            "value": {"loads": len(o["points"].get(loads_metric, [])),
                      "exit_rc": (o["exit"] or {}).get("rc")},
            "bar": "start .. end frames, summary = stream, exit rc 0",
            "result": "FAIL" if why else "PASS", **({"why": "; ".join(why)} if why else {})}


def k6_checks(o: dict | None, th: dict | None = None) -> list[dict]:
    th = dict(K6_THRESHOLDS, **(th or {}))
    out = [_stream_check(o, "k6", "page_loads")]
    if o is None or o["summary"] is None:
        return out
    s = o["summary"]
    scenarios = sorted({k.split("scenario:", 1)[1].rstrip("}")
                        for k in (s.get("metrics") or {}) if k.startswith("iterations{scenario:")})
    cfg = s.get("config") or {}
    # run 12: one VU per user (navigate.js session); a user's arrival is on
    # an open schedule, a start later than LATE_MS is counted and every user
    # must have arrived. Arrival-rate scenarios (run 11's streams) are judged
    # by their dropped iterations, below.
    users = {"navigate": cfg.get("navUsers"), "composite_burst": cfg.get("burstUsers")}
    for sc in scenarios:
        if f"arrivals{{scenario:{sc}}}" not in (s.get("metrics") or {}):
            continue
        arrived = _metric(s, f"arrivals{{scenario:{sc}}}", "count") or 0
        late = _metric(s, f"arrivals_late{{scenario:{sc}}}", "count") or 0
        want = users.get(sc) or 0
        value = round(late / arrived, 5) if arrived else None
        why = []
        if arrived < want:
            why.append(f"only {arrived} of {want} {sc} users arrived: the others' sessions never "
                       f"started")
        if value is None or value > th["max_dropped_share"]:
            why.append(f"{late} of {arrived} {sc} first loads started more than 1 s after their "
                       f"arrival slot: the load generator, not snowplow, fell behind"
                       if arrived else f"no {sc} user arrived: that load never ran")
        out.append({"check": f"k6.late_arrivals[{sc}]", "value": value,
                    "bar": f"<= {th['max_dropped_share']} late, all {want} arrived",
                    "result": "FAIL" if why else "PASS", **({"why": "; ".join(why)} if why else {})})
        # #839 gate: a click is closed-loop (previous page end + THINK_S); one
        # that started more than 1 s after that is k6 falling behind, which
        # would otherwise just lower the offered load
        clicks = _metric(s, f"clicks{{scenario:{sc}}}", "count") or 0
        clate = _metric(s, f"clicks_late{{scenario:{sc}}}", "count") or 0
        open_ = _metric(s, f"clicks_open_schedule{{scenario:{sc}}}", "count") or 0
        cval = round(clate / clicks, 5) if clicks else None
        cwhy = (f"{clate} of {clicks} {sc} route clicks started more than 1 s after their "
                f"intended time (previous page end + think time): the load generator, not "
                f"snowplow, fell behind and the offered load dropped"
                if clicks and cval > th["max_dropped_share"] else
                f"no {sc} route click ran, though the run had time for {open_}"
                if not clicks and open_ else None)
        out.append({"check": f"k6.late_clicks[{sc}]", "value": cval,
                    "bar": f"<= {th['max_dropped_share']} of the clicks late",
                    "result": "FAIL" if cwhy else "PASS", **({"why": cwhy} if cwhy else {})})
    for sc in scenarios:
        if sc == "composite_burst" or f"arrivals{{scenario:{sc}}}" in (s.get("metrics") or {}):
            continue                     # per-vu-iterations never drops: arrivals above
        done = _metric(s, f"iterations{{scenario:{sc}}}", "count") or 0
        dropped = _metric(s, f"dropped_iterations{{scenario:{sc}}}", "count") or 0
        scheduled = done + dropped
        value = round(dropped / scheduled, 5) if scheduled else None
        ok = value is not None and value <= th["max_dropped_share"]
        bar = f"<= {th['max_dropped_share']} of {scheduled} scheduled"
        why = (f"k6 could not start {dropped} of {scheduled} scheduled {sc} iterations on "
               f"time (no free VU): the load generator, not snowplow, fell behind"
               if scheduled else f"k6 scheduled no {sc} iteration: that load never ran")
        out.append({"check": f"k6.dropped_iterations[{sc}]", "value": value, "bar": bar,
                    "result": "PASS" if ok else "FAIL", **({} if ok else {"why": why})})
    burst = ((s.get("config") or {}).get("burstUsers")) or 0
    if burst:
        # every burst user's deep link ran to a recorded page load; one cut
        # by the scenario's maxDuration, or never started, is missing here
        # (run 12's burst users also click: count their first loads only)
        key = ("first_loads{scenario:composite_burst}"
               if "first_loads{scenario:composite_burst}" in (s.get("metrics") or {})
               else "page_loads{scenario:composite_burst}")
        loads = _metric(s, key, "count") or 0
        ok = loads == burst
        out.append({"check": "k6.composite_burst_complete", "value": loads, "bar": f"== {burst}",
                    "result": "PASS" if ok else "FAIL",
                    **({} if ok else {"why": f"only {loads} of the {burst} composite-burst users' "
                                             f"page loads completed: the burst was not whole"})})
    rate = _metric(s, "http_req_failed", "rate")
    out.append({"check": "k6.error_rate", "value": rate, "bar": f"<= {th['max_k6_error_rate']}",
                "result": "PASS" if rate is not None and rate <= th["max_k6_error_rate"] else "FAIL",
                **({} if rate is not None else {"why": "no data"})})
    mism = _metric(s, "route_table_mismatch", "count") or 0
    out.append({"check": "k6.route_table", "value": mism, "bar": "== 0",
                "result": "PASS" if mism == 0 else "FAIL",
                **({} if mism == 0 else {"why": f"{mism} users' own menus differ from the run's "
                                                f"route table (preflight's): their route clicks "
                                                f"would not be the SPA's"})})
    return out


def browser_checks(o: dict | None, sessions: int | None, th: dict | None = None) -> list[dict]:
    th = dict(K6_THRESHOLDS, **(th or {}))
    out = [_stream_check(o, "browser", "browser_page_loads")]
    if o is None:
        return out
    logged = {t.get("session") for _, t in o["points"].get("browser_logins", [])}
    out.append({"check": "browser.sessions", "value": len(logged),
                "bar": f">= {th['min_browser_sessions']}" + (f" of {sessions}" if sessions else ""),
                "result": "PASS" if len(logged) >= th["min_browser_sessions"] else "FAIL",
                **({} if len(logged) >= th["min_browser_sessions"] else
                   {"why": f"only {len(logged)} real-browser sessions signed in and loaded "
                           f"their first page"})})
    per = Counter(t.get("session") for _, t in o["points"].get("browser_page_loads", []))
    for sess in sorted(logged):
        n = per.get(sess, 0)
        ok = n >= th["min_browser_loads_per_session"]
        out.append({"check": f"browser.page_loads[{sess}]", "value": n,
                    "bar": f">= {th['min_browser_loads_per_session']}",
                    "result": "PASS" if ok else "FAIL"})
    opened = defaultdict(int)
    for val, t in o["points"].get("browser_sse_opened", []):
        opened[(t.get("session"), t.get("stream"))] += int(val or 0)
    for sess in sorted(logged):
        have = {s: opened.get((sess, s), 0) for s in ("refreshes", "notifications")}
        low = [s for s, n in have.items() if n < th["min_sse_opened_per_stream"]]
        out.append({"check": f"browser.live_refresh[{sess}]", "value": have,
                    "bar": f">= {th['min_sse_opened_per_stream']} opened per stream",
                    "result": "FAIL" if low else "PASS",
                    **({"why": "no live-update stream opened: " + ", ".join(
                        f"/{s}" for s in low) + " (the session saw no SSE path)"} if low else {})})
    return out


def read_viewer_out(d: str) -> dict | None:
    """The viewer sidecar's records and exit record in collected dir d."""
    path, exitf = os.path.join(d, "viewer.jsonl"), os.path.join(d, "viewer-exit.json")
    if not (os.path.exists(path) or os.path.exists(exitf)):
        return None
    out = {"records": [], "truncated": False, "exit": None}
    try:
        with open(path) as fh:
            for line in fh:
                try:
                    out["records"].append(json.loads(line))
                except ValueError:
                    out["truncated"] = True
    except OSError:
        pass
    try:
        with open(exitf) as fh:
            out["exit"] = json.load(fh)
    except (OSError, ValueError):
        pass
    return out


def _active_users(k6o: dict | None) -> int | None:
    """Users k6 brought to a working portal: its successful first loads (one
    per user), from k6's own stream."""
    if k6o is None:
        return None
    p = k6o["points"]
    firsts = sum(1 for _, t in p.get("page_loads", [])
                 if t.get("kind") in ("first_load", "composite_first_load"))
    return firsts - int(sum(v or 0 for v, _ in p.get("first_load_failed", [])))


def viewer_checks(v: dict | None, k6o: dict | None, th: dict | None = None) -> list[dict]:
    """The open tabs: the sidecar's stream is whole and ended at k6's
    feed_end, and >= 90 % of the users k6 brought in were armed."""
    th = dict(K6_THRESHOLDS, **(th or {}))
    if v is None:
        return [{"check": "viewer.stream_complete", "value": None, "bar": "collected",
                 "result": "FAIL", "why": "no viewer output was collected: no open tabs"}]
    recs = v["records"]
    kinds = [r.get("kind") for r in recs]
    end = next((r for r in reversed(recs) if r.get("kind") == "viewer_end"), None)
    why = []
    if not recs or kinds[0] != "viewer_start":
        why.append("no viewer_start first: the stream's head is missing")
    if end is None:
        why.append("no viewer_end: the sidecar did not finish, or the tail is missing")
    elif end.get("ended_by") != "feed_end":
        why.append(f"the viewers stopped on: {end.get('ended_by')}")
    if v["truncated"]:
        why.append("the stream is cut (an unreadable line)")
    if v["exit"] is None:
        why.append("no exit record: the sidecar died before its wrapper recorded the exit code")
    elif v["exit"].get("rc") != 0:
        why.append(f"the viewer sidecar exited rc={v['exit'].get('rc')}")
    out = [{"check": "viewer.stream_complete",
            "value": {"navs": (end or {}).get("navs"), "exit_rc": (v["exit"] or {}).get("rc")},
            "bar": "viewer_start .. viewer_end at feed_end, exit rc 0",
            "result": "FAIL" if why else "PASS", **({"why": "; ".join(why)} if why else {})}]
    armed = len({r.get("user") for r in recs if r.get("kind") == "viewer_armed"})
    active = _active_users(k6o)
    share = round(armed / active, 4) if active else None
    ok = share is not None and share >= th["min_armed_user_share"]
    out.append({"check": "viewer.armed_users", "value": {"armed": armed, "active": active,
                                                          "share": share},
                "bar": f">= {th['min_armed_user_share']} of the users k6 brought in",
                "result": "PASS" if ok else "FAIL",
                **({} if ok else {"why": f"{armed} users had an open tab of {active} that k6 "
                                         f"brought to a working portal"
                                  if active else "k6 brought no user to a working portal"})})
    return out


def viewer_report(v: dict | None) -> dict | None:
    """Arm lag (k6 nav end -> stream open) and push refetches: report only."""
    if v is None:
        return None
    from bench.load606 import push_refetch_report
    recs = v["records"]
    lag = [r.get("arm_lag_ms") for r in recs if r.get("kind") == "viewer_armed"]
    stats = next((r for r in reversed(recs) if r.get("kind") == "viewer_stats"), None)
    dead = [r for r in recs if r.get("kind") == "viewer_dead"]
    return {"arms": len(lag), "arm_lag_p50_ms": _pct(lag, 50), "arm_lag_p95_ms": _pct(lag, 95),
            "arm_lag_max_ms": max(lag) if lag else None,
            "push_refetch": push_refetch_report([r for r in recs if r.get("kind") == "push_refetch"]),
            "stream_errors": sum(1 for r in recs if r.get("kind") == "viewer"
                                 and r.get("state") == "error"),
            "stats": {k: val for k, val in (stats or {}).items() if k not in ("kind", "t")} or None,
            "tabs_dead_last": dead[-1].get("tabs_dead") if dead else None}


def browser_navs(o: dict | None) -> list[dict]:
    """The real-browser page loads as nav records for the north-star checks
    (visit 1 = this session's first load of the route, 2 = warm)."""
    if o is None:
        return []
    times = (o.get("times") or {}).get("browser_page_load_ms") or []
    return [{"kind": "nav", "nav": t.get("method"), "page": t.get("route"),
             "session": t.get("session"), "wall_ms": round(v, 1),
             "visit": 1 if t.get("visit") == "first" else 2, "calls": 0, "errors": 0,
             # the point is written when the load ends
             **({"start": times[i] - v / 1000} if i < len(times) and times[i] else {})}
            for i, (v, t) in enumerate(o["points"].get("browser_page_load_ms", []))]


def k6_page_loads(o: dict | None) -> list[tuple[float, float, dict]]:
    """k6's page loads as (start epoch, ms, tags), those with a time."""
    if o is None:
        return []
    times = (o.get("times") or {}).get("page_load_ms") or []
    return [(times[i] - v / 1000, v, t) for i, (v, t) in enumerate(o["points"].get("page_load_ms", []))
            if i < len(times) and times[i] and v is not None]


def _by(points, *keys):
    g = defaultdict(list)
    for v, t in points:
        g[tuple(t.get(k) for k in keys)].append(v)
    return {"/".join(str(x) for x in k): {"n": len(v), "p50_ms": _pct(v, 50), "p95_ms": _pct(v, 95),
                                          "max_ms": max(v)}
            for k, v in sorted(g.items(), key=lambda kv: tuple(str(x) for x in kv[0]))}


def _clicks(summary: dict, sc: str) -> dict:
    done = _metric(summary, f"clicks{{scenario:{sc}}}", "count") or 0
    open_ = _metric(summary, f"clicks_open_schedule{{scenario:{sc}}}", "count") or 0
    return {"done": done, "late": _metric(summary, f"clicks_late{{scenario:{sc}}}", "count") or 0,
            "open_schedule": open_,
            "achieved_share_of_open_schedule": round(done / open_, 4) if open_ else None}


def k6_report(o: dict | None) -> dict | None:
    """k6's page and call timings, reported beside the browser's (not gated)."""
    if o is None:
        return None
    p = o["points"]
    return {"page_load_by_kind_visit": _by(p.get("page_load_ms", []), "kind", "visit"),
            "page_load_by_route": _by(p.get("page_load_ms", []), "route"),
            "call_ms_by_level": _by(p.get("http_req_duration", []), "level"),
            "first_load_failed": sum(v for v, _ in p.get("first_load_failed", [])),
            "page_errors": sum(v for v, _ in p.get("page_errors", [])),
            # run 12: route clicks the open schedule would have started while
            # the user's first load still ran (navigate.js defers them)
            "clicks_deferred": {
                k.split("scenario:", 1)[1].rstrip("}"): v.get("count")
                for k, v in ((o["summary"] or {}).get("metrics") or {}).items()
                if k.startswith("clicks_deferred{scenario:")},
            "arrival_lag_ms": ((o["summary"] or {}).get("metrics") or {}).get("arrival_lag_ms"),
            "click_lag_ms": ((o["summary"] or {}).get("metrics") or {}).get("click_lag_ms"),
            # achieved route clicks vs run 11's open schedule (one per THINK_S
            # from the arrival): how much offered load the SPA-like clicks
            # leave, to compare run 12's load with run 11's
            "clicks": {sc: _clicks(o["summary"], sc) for sc in ("navigate", "composite_burst")
                       if _metric(o["summary"], f"clicks{{scenario:{sc}}}", "count") is not None},
            "summary": (o["summary"] or {}).get("metrics")}


def browser_report(o: dict | None) -> dict | None:
    if o is None:
        return None
    p = o["points"]
    states = defaultdict(int)
    for v, t in p.get("browser_widget_states", []):
        states[t.get("state")] += int(v or 0)
    return {"first_visit_is": ((o.get("summary") or {}).get("firstVisit")),
            "page_load_by_session": _by(p.get("browser_page_load_ms", []), "session"),
            "page_load_by_route_visit": _by(p.get("browser_page_load_ms", []), "route", "visit"),
            # skeleton=no: painted from the app's own cache, no request (pageload.js)
            "page_load_by_visit_skeleton": _by(p.get("browser_page_load_ms", []), "visit", "skeleton"),
            # the sign-in (submit -> the app leaves /login): authn, not a page load
            "signin_by_session": _by(p.get("browser_signin_ms", []), "session"),
            # all sessions together (run 11's reader found no flat value)
            "signin_ms": (lambda v: {"n": len(v), "p50_ms": _pct(v, 50), "p95_ms": _pct(v, 95),
                                     "max_ms": max(v)} if v else None)(
                [x for x, _ in p.get("browser_signin_ms", [])]),
            "forward_wait": o.get("forward"),
            "timeouts": sum(v for v, _ in p.get("browser_page_timeouts", [])),
            "widget_states": dict(states),
            "cpu": cpu_stats((o.get("cpu") or {}).get("browser"))}


def widget_error_check(o: dict | None, th: dict | None = None) -> dict:
    """Page loads that ended with a widget-error / widget-timeout state, as a
    share of the browser page loads (at most: one point per state per load)."""
    th = dict(K6_THRESHOLDS, **(th or {}))
    p = (o or {}).get("points") or {}
    bad = sum(1 for _, t in p.get("browser_widget_states", []) if t.get("state") in ("error", "timeout"))
    loads = len(p.get("browser_page_loads", []))
    share = round(bad / loads, 4) if loads else None
    ok = share is not None and share <= th["max_widget_error_share"]
    return {"check": "browser.widget_errors", "value": {"loads_with_error_state": bad, "loads": loads,
                                                         "share": share},
            "bar": f"<= {th['max_widget_error_share']} of browser page loads",
            "result": "PASS" if ok else "FAIL",
            **({} if ok else {"why": f"{bad} of {loads} browser page loads ended with a widget "
                                     f"error/timeout state: they did not load their content"
                              if loads else "no browser page load"})}
