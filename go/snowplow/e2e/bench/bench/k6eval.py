"""Read and judge run 11's k6 outputs (#830): the k6 background load and the
real-Chrome page loads.

Each pod leaves, under its collected /out:
  <name>.json.gz        k6's metric stream (--out json): one JSON object per
                        line, `{"type": "Point", "metric", "data": {value,
                        tags, time}}`, framed by bench_frame{frame:start|end}
  <name>-summary.json   handleSummary's totals (the evaluator matches the
                        stream's page-load count against it)
  <name>-exit.json      the wrapper's exit record

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

__all__ = [
    "browser_checks",
    "browser_navs",
    "browser_report",
    "k6_checks",
    "k6_report",
    "read_k6_out",
]

# Bars (THRESHOLDS-style: written here, never derived from the instrument).
K6_THRESHOLDS = {
    # arrival-rate starts k6 could not make, as a share of the scheduled ones
    "max_dropped_share": 0.001,
    # the composite burst is ONE instant: any drop there is INVALID
    "max_dropped_burst": 0,
    "max_k6_error_rate": 0.01,
    "min_browser_sessions": 3,
    "min_browser_loads_per_session": 20,
}

_KEEP = ("page_load_ms", "page_loads", "page_errors", "first_load_failed",
         "route_table_mismatch", "bench_frame", "http_req_failed", "http_req_duration",
         "browser_page_load_ms", "browser_page_loads", "browser_widget_errors",
         "browser_page_timeouts", "browser_logins", "dropped_iterations")
_METRIC = re.compile(r'\{"metric":"([^"]+)"')


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
        except (OSError, EOFError):
            out["truncated"] = True                      # a cut gzip member
    for key, path in (("summary", summary), ("exit", exitf)):
        try:
            with open(path) as fh:
                out[key] = json.load(fh)
        except (OSError, ValueError):
            pass
    return out


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
    for sc in scenarios:
        done = _metric(s, f"iterations{{scenario:{sc}}}", "count") or 0
        dropped = _metric(s, f"dropped_iterations{{scenario:{sc}}}", "count") or 0
        scheduled = done + dropped
        if sc == "composite_burst":
            ok = dropped <= th["max_dropped_burst"]
            bar = f"<= {th['max_dropped_burst']} (the burst is one instant)"
            value = dropped
        else:
            value = round(dropped / scheduled, 5) if scheduled else None
            ok = value is not None and value <= th["max_dropped_share"]
            bar = f"<= {th['max_dropped_share']} of {scheduled} scheduled"
        why = (f"k6 could not start {dropped} of {scheduled} scheduled {sc} iterations on "
               f"time (no free VU): the load generator, not snowplow, fell behind"
               if scheduled else f"k6 scheduled no {sc} iteration: that load never ran")
        out.append({"check": f"k6.dropped_iterations[{sc}]", "value": value, "bar": bar,
                    "result": "PASS" if ok else "FAIL", **({} if ok else {"why": why})})
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
    return out


def browser_navs(o: dict | None) -> list[dict]:
    """The real-browser page loads as nav records for the north-star checks
    (visit 1 = this session's first load of the route, 2 = warm)."""
    if o is None:
        return []
    return [{"kind": "nav", "nav": t.get("method"), "page": t.get("route"),
             "session": t.get("session"), "wall_ms": round(v, 1),
             "visit": 1 if t.get("visit") == "first" else 2, "calls": 0, "errors": 0}
            for v, t in o["points"].get("browser_page_load_ms", [])]


def _by(points, *keys):
    g = defaultdict(list)
    for v, t in points:
        g[tuple(t.get(k) for k in keys)].append(v)
    return {"/".join(str(x) for x in k): {"n": len(v), "p50_ms": _pct(v, 50), "p95_ms": _pct(v, 95),
                                          "max_ms": max(v)} for k, v in sorted(g.items())}


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
            "summary": (o["summary"] or {}).get("metrics")}


def browser_report(o: dict | None) -> dict | None:
    if o is None:
        return None
    p = o["points"]
    return {"page_load_by_session": _by(p.get("browser_page_load_ms", []), "session"),
            "page_load_by_route_visit": _by(p.get("browser_page_load_ms", []), "route", "visit"),
            # skeleton=no: painted from the app's own cache, no request (browser.js)
            "page_load_by_visit_skeleton": _by(p.get("browser_page_load_ms", []), "visit", "skeleton"),
            "timeouts": sum(v for v, _ in p.get("browser_page_timeouts", [])),
            "widget_errors": sum(v for v, _ in p.get("browser_widget_errors", []))}
