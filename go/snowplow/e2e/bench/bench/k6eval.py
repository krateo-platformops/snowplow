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
    # Chrome's CPU p95 against this share of its request: REPORTED only, it
    # never voids a run (Diego, browser-starvation ruling). The request is a
    # scheduling reservation (guarded by k6_job.loadgen_fit); Chrome has no
    # limit, so using more than it is not starvation. Starvation is MEASURED:
    # see starvation() and load606.starvation_check.
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
         "dropped_iterations", "clicks", "arrivals",
         # the browser's completed requests: the pair's customer witness reads them
         "browser_http_req_duration", "browser_http_req_failed",
         "browser_call_ms", "browser_request_incomplete_ms", "browser_request_failed_start_unseen",
         "browser_fresh_class",
         # #1508: one per browser session, how it ended (early yes/no, its cause)
         "browser_session_end",
         # #1517: each wait for a renderer read (before a class's sign-in, after its last load): outside every load
         "browser_renderer_wait_ms")
_METRIC = re.compile(r'\{"metric":"([^"]+)"')
# the points whose time is kept too (out["times"], index-aligned with
# out["points"]): run 12 compares latency inside / outside the CPU profiles;
# run 15's customer-latency gate (custlat) puts every call, click and arrival
# in its phase; the paired gate counts k6's errors (http_req_failed) only
# inside the time both runs' snowplow was Ready
_TIMED = ("page_load_ms", "browser_page_load_ms", "http_req_duration", "http_req_failed", "clicks",
          "arrivals", "browser_http_req_duration", "browser_http_req_failed", "browser_call_ms",
          "browser_request_incomplete_ms", "browser_request_failed_start_unseen",
          "browser_fresh_class",
          # 2026-11-17: when a session ended (a lost one is judged by what overlapped its last stretch)
          "browser_session_end")
# the points whose sample metadata is kept too (out["metadata"], index-aligned
# with out["points"]; {} when a point has none): #1089's per-call Resource
# Timing rides as browser_call_ms' call_timing metadata (never a tag), a page
# load's id as its own points' load_id, and the VU as every browser sample's
# `vu` (bench/calltiming.py joins Chrome's request samples to a load by it)
_WITH_METADATA = ("browser_call_ms", "browser_page_load_ms", "browser_http_req_duration")
_NO_METADATA: dict = {}          # shared, never written: most points carry none
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
           "times": defaultdict(list), "metadata": defaultdict(list),
           "frames": Counter(), "truncated": False, "summary": None, "exit": None}
    # one shared dict per distinct tag set: a paired run's customers stay on
    # for hours (K6_SPAN=churn, millions of calls with a few dozen tag sets)
    tagsets: dict[tuple, dict] = {}
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
                    tags = tagsets.setdefault(tuple(sorted(tags.items())), tags)
                    if r["metric"] == "bench_frame":
                        out["frames"][tags.get("frame")] += 1
                        # #1520 review: when the drive started (setup's start frame), a lost session's window's
                        # earliest start
                        out.setdefault("frame_times", {}).setdefault(tags.get("frame"), []).append(
                            epoch(data.get("time")))
                    else:
                        out["points"][r["metric"]].append((data.get("value"), tags))
                        if r["metric"] in _TIMED:
                            out["times"][r["metric"]].append(epoch(data.get("time")))
                        if r["metric"] in _WITH_METADATA:
                            out["metadata"][r["metric"]].append(data.get("metadata") or _NO_METADATA)
        except (OSError, EOFError):
            out["truncated"] = True                      # a cut gzip member
    for key, path in (("summary", summary), ("exit", exitf)):
        try:
            with open(path) as fh:
                out[key] = json.load(fh)
        except (OSError, ValueError):
            pass
    # the browser pod's renderers sidecar records its own cgroup too (Diego 2026-11-17: the helper measured directly)
    out["cpu"] = {c: read_cpu(d, c) for c in {"k6": ("k6", "viewer"), "browser": ("browser", "renderers")}.get(
        name, (name,))}
    if name == "browser":
        # 2026-11-17 ("Measure it precisely"): each renderer's run-queue wait, and which renderer renders each
        # session's page (None: not recorded, a bench before it)
        out["renderers"] = _jsonl(os.path.join(d, "browser-renderers.jsonl"))
        out["renderer_map"] = _jsonl(os.path.join(d, "renderer-map.jsonl"))
    try:
        with open(os.path.join(d, f"{name}-forward.json")) as fh:
            out["forward"] = json.load(fh)   # the browser wrapper's wait for its forwards
    except (OSError, ValueError):
        out["forward"] = None
    return out


def _jsonl(path: str) -> list[dict] | None:
    """The JSON object lines of `path` (a cut last line skipped); None when there is no such file."""
    try:
        with open(path, errors="replace") as fh:
            lines = fh.read().splitlines()
    except OSError:
        return None
    out = []
    for ln in lines:
        try:
            r = json.loads(ln)
        except ValueError:
            continue
        if isinstance(r, dict):
            out.append(r)
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




#: /proc/uptime prints centiseconds, truncated: a read can be up to 10 ms behind the instant
UPTIME_RES_S = 0.01


def _anchor(r: dict) -> tuple[float, float] | None:
    """The interval the true realtime - boottime offset lies in, from one
    tick-bracketed anchor (k6_job._cpu_sampler's `anchor`): realtime crossed
    `epoch` between the uptime reads `uptime_lo` and `uptime`, each up to
    UPTIME_RES_S behind its instant, so the offset is in
    [epoch - uptime - UPTIME_RES_S, epoch - uptime_lo]. None if not recorded."""
    if not all(r.get(k) is not None for k in ("epoch", "uptime", "uptime_lo")):
        return None
    e, u, lo = float(r["epoch"]), float(r["uptime"]), float(r["uptime_lo"])
    return e - u - UPTIME_RES_S, e - min(lo, u)


def starvation(o: dict | None, container: str = "browser") -> dict:
    """The container's measured starvation evidence, per sample window:
    `stall_s` (cgroup cpu.pressure "some": seconds during which at least one
    of its runnable tasks waited for a CPU) and `saturated` (the NODE had not
    one idle tick in the window: /proc/stat's idle + iowait did not move, to
    its 10 ms tick, on any CPU). Windows are on the epoch (cpu_start's
    anchor: the tick of `date +%s`, bracketed by two /proc/uptime reads).
    `slack_s` is that bracket plus /proc/uptime's 10 ms resolution, the
    anchor's whole error, measured per run (#1043 review B1, round 2): each
    window is widened by it. cpu_end carries a second anchor: `clock_step_s`
    is how far the two disagree past their brackets (a realtime step during
    the drive; NTP slewing moves realtime and boottime alike, so a steady
    clock reads 0). `pressure` / `node` / `anchored` / `end_anchored` say
    whether each was MEASURED: a series that lacks it (one null sample
    included) is unmeasured, never "not starved"."""
    recs = ((o or {}).get("cpu") or {}).get(container) or []
    start = next((r for r in recs if r.get("kind") == "cpu_start"), {})
    end = next((r for r in recs if r.get("kind") == "cpu_end"), {})
    pts = [r for r in recs if r.get("kind") == "cpu" and r.get("usage_usec") is not None]
    pressure = bool(pts) and all(p.get("pressure_some_usec") is not None for p in pts)
    node = bool(pts) and all(p.get("node_idle_ticks") is not None and p.get("node_busy_ticks") is not None
                             for p in pts)
    a0, a1 = _anchor(start), _anchor(end)
    anchored = a0 is not None
    off = float(start["epoch"]) - float(start["uptime"]) if anchored else 0.0
    slack = max(0.0, float(start["uptime"]) - float(start["uptime_lo"])) + UPTIME_RES_S if anchored else None
    # the two anchors' offset intervals: apart = the clock stepped by at least the gap
    step = round(max(0.0, a1[0] - a0[1], a0[0] - a1[1]), 3) if anchored and a1 is not None else None
    wins = []
    for a, b in pairwise(pts):
        dt = float(b["t"]) - float(a["t"])
        if dt <= 0:
            continue
        w = {"t0": float(a["t"]) + off, "t1": float(b["t"]) + off}
        if pressure:
            w["stall_s"] = max(0, b["pressure_some_usec"] - a["pressure_some_usec"]) / 1e6
            w["full_s"] = (max(0, b["pressure_full_usec"] - a["pressure_full_usec"]) / 1e6
                           if a.get("pressure_full_usec") is not None and b.get("pressure_full_usec") is not None
                           else None)
        if node:
            di = b["node_idle_ticks"] - a["node_idle_ticks"]
            db = b["node_busy_ticks"] - a["node_busy_ticks"]
            w["saturated"] = di <= 0 and db > 0
            w["node_busy_share"] = round(db / (db + di), 4) if db + di > 0 else None
        wins.append(w)
    span = sum(w["t1"] - w["t0"] for w in wins)
    # #1525 review: the per-renderer rule is decided by what the run was CONFIGURED to do (its recorded browser
    # config's renderer watch, o["renderer_watch"]: renderer_watch_of), never by which of its files exist: a pod
    # with the watch whose sidecar wrote nothing is judged per renderer, its loads UNKNOWN, never as old evidence
    rend = (renderer_state(o, off if anchored else None)
            if container == "browser" and (o or {}).get("renderer_watch") == WATCH_ON else None)
    return {"rule": RULE_RENDERER if rend is not None else RULE_CONTAINER, "renderer": rend,
            "container": container, "pressure": pressure, "node": node, "anchored": anchored,
            "end_anchored": a1 is not None, "clock_step_s": step,
            "slack_s": slack, "node_cpus": start.get("node_cpus"), "windows": wins,
            "stall_share": (round(sum(w["stall_s"] for w in wins) / span, 4) if pressure and span else None),
            "saturated_windows": (sum(1 for w in wins if w["saturated"]) if node else None)}


#: the two rules a page load's possible delay is judged by (browser.cpu_starvation says which judged a run)
RULE_RENDERER = "per renderer"
RULE_CONTAINER = "container window"
RULE_TEXT = {
    RULE_RENDERER: ("per renderer (2026-11-17): each load charged its OWN renderer process's run-queue wait "
                    "(/proc/<pid>/task/*/schedstat run_delay) between the samples that bracket it, the renderer named "
                    "by Chrome's frame->process record (k6/renderers.js)"),
    RULE_CONTAINER: ("container window (evidence without per-renderer data, before 2026-11-17): each 5 s window's "
                     "whole browser-container cgroup stall charged to every load overlapping it"),
}


def renderer_state(o: dict | None, off: float | None) -> dict:
    """The per-renderer evidence of one browser run: each renderer's run_delay series on the epoch, KEYED BY
    (pid, starttime) (`off`: the CPU samples' realtime - uptime offset; None: not anchored, nothing can be placed),
    each series' --user-data-dir, and every renderer read (k6/renderers.js), the failed ones too (pid null: they
    break a chain of agreeing reads)."""
    series: dict[tuple[int, str], list[tuple[float, int]]] = {}
    threads: dict[tuple[int, str], dict[float, dict[tuple[str, str], int]]] = {}
    udd: dict[tuple[int, str], str | None] = {}
    per_thread = False
    for r in (o or {}).get("renderers") or []:
        if r.get("kind") == "renderer_udd" and isinstance(r.get("pid"), int):
            udd[(r["pid"], str(r.get("start")))] = r.get("udd")
        elif r.get("kind") == "renderer" and isinstance(r.get("t"), (int, float)) and off is not None:
            per_thread = per_thread or "th" in r
            for k, v in (r.get("rd") or {}).items():
                pid, _, start = str(k).partition(":")
                if pid.isdigit() and start:
                    series.setdefault((int(pid), start), []).append((float(r["t"]) + off, int(v)))
            for k, ths in (r.get("th") or {}).items():
                pid, _, start = str(k).partition(":")
                if pid.isdigit() and start and isinstance(ths, dict):
                    threads.setdefault((int(pid), start), {})[float(r["t"]) + off] = {
                        tuple(str(tk).partition(":")[::2]): int(tv) for tk, tv in ths.items()}
    for s in series.values():
        s.sort(key=lambda x: x[0])
    # each series' process start on the epoch (its starttime / the sampler's CLK_TCK, on the same uptime the samples
    # are timed by): the earliest instant the process is known alive, before its first sample
    hz = next((r.get("hz") for r in (o or {}).get("renderers") or [] if r.get("kind") == "renderer_clock"), None)
    started = ({k: int(k[1]) / hz + off for k in series if k[1].isdigit()}
               if isinstance(hz, int) and hz > 0 and off is not None else {})
    reads = [m for m in (o or {}).get("renderer_map") or []
             if m.get("kind") == "renderer_map" and isinstance(m.get("t0"), (int, float))
             and isinstance(m.get("t1"), (int, float))]
    return {"series": series, "threads": threads, "per_thread": per_thread, "udd": udd, "started": started,
            "reads": sorted(reads, key=lambda m: m["t0"]),
            "anchored": off is not None, "recorded": {"renderers": (o or {}).get("renderers") is not None,
                                                     "renderer_map": (o or {}).get("renderer_map") is not None}}


def _series_of(r: dict, pid: int, t0: float, t1: float) -> tuple[tuple[int, str] | None, str]:
    """The one (pid, starttime) series of `pid` that was sampled through a read lasting [t0, t1]: a pid's series
    ends where its process ended; another process that got the pid later is another series. Known alive from its
    process's own start (its starttime, r["started"]) or else its first sample, to its last sample."""
    started = r.get("started") or {}
    alive = [k for k, s in r["series"].items()
             if k[0] == pid and min(s[0][0], started.get(k, s[0][0])) <= t1 and s[-1][0] >= t0]
    if len(alive) != 1:
        return None, (f"{len(alive)} sampled processes held pid {pid} through its read" if alive
                      else f"no renderer sample of pid {pid} spans its read")
    return alive[0], ""


def renderer_delay_ms(n: dict, st: dict) -> tuple[float | None, str]:
    """renderer_delay's (ms, why): the stall it measured (without its under-count bound), or None and why."""
    d = renderer_delay(n, st)
    return d["ms"], d["why"]


def _thread_delta(times: list[float], snaps: list[dict]) -> tuple[dict | None, str]:
    """A load's renderer run_delay from each THREAD's own samples (k6_job._renderer_sampler's "th"), over the samples
    that bracket the load (`times`, `snaps`: {(tid, thread starttime): ns} per sample): the sum over threads of each
    thread's last sampled value less its first. A thread that exits keeps its last sampled value (the sum never
    decreases because of an exit), one first seen after the first sample counts from its first sample. What the sum
    cannot hold, it bounds: an exited thread's run_delay after its last sample, and a new thread's before its first,
    are each at most the time between that sample and its neighbour (one thread waits at most the wall time). A
    thread (the same tid AND starttime) whose run_delay went backwards is a broken instrument: (None, why)."""
    total, bound_s, exited, started = 0, 0.0, 0, 0
    for key in sorted({k for s in snaps for k in s}):
        idx = [i for i, s in enumerate(snaps) if key in s]
        vals = [snaps[i][key] for i in idx]
        if any(b < a for a, b in pairwise(vals)):
            return None, (f"thread {key[0]} (starttime {key[1]}) of its renderer: its own run_delay went backwards "
                          "across it (a broken counter, not an exit)")
        total += vals[-1] - vals[0]
        if idx[-1] < len(snaps) - 1:
            exited += 1
            bound_s += times[idx[-1] + 1] - times[idx[-1]]
        if idx[0] > 0:
            started += 1
            bound_s += times[idx[0]] - times[idx[0] - 1]
    return {"ns": total, "bound_ms": bound_s * 1000, "exited": exited, "started": started}, ""


def renderer_delay(n: dict, st: dict) -> dict:
    """{"ms": at most how much of page load `n` was its OWN renderer waiting for a CPU, "bound_ms": what that may
    under-count (threads that exited or started inside its bracket: _thread_delta), "upper_ms": the two together
    (capped at the load's time), "exited", "started": those threads, "why": ""}, or "ms" None and why it is UNKNOWN.

    Its renderer: the pid Chrome's frame->process record named for the load's session and identity class in a read
    that ENDED before the load started and in one that STARTED after it ended, the same pid and page target in
    both and in every read between them, with no failed read of that Chrome between them (a renderer that changed,
    or that was not read on both sides, is not known), resolved to the one (pid, starttime) series the sampler
    recorded through both reads, under the --user-data-dir of the Chrome whose endpoint named it. Its stall: that
    series' run_delay between the last sample at or before the load's start and the first at or after its end (each
    side widened OUTWARD by the anchor's error, st["slack_s"]), capped at the load's own time. With per-thread samples
    (a sampler since 2026-11-18) the stall is the sum of each thread's own delta (_thread_delta): a thread that exits
    is a stated under-count bound, never UNKNOWN; a thread whose own run_delay went backwards is UNKNOWN. Without
    them (older evidence) the series' summed run_delay, and a sum that decreased across the load is UNKNOWN (a
    thread exited with its run_delay: #1517's known under-count, which only per-thread samples can resolve).
    UNKNOWN too: a series whose run_delay never advanced over the run (the counter is not shown live)."""
    def unknown(why: str) -> dict:
        return {"ms": None, "bound_ms": None, "upper_ms": None, "exited": 0, "started": 0, "why": why}

    r = st.get("renderer") or {}
    if not r.get("anchored"):
        return unknown("the renderer samples have no wall-clock anchor")
    if not (r.get("recorded") or {}).get("renderers"):
        return unknown("no renderer samples were recorded (browser-renderers.jsonl)")
    if not (r.get("recorded") or {}).get("renderer_map"):
        return unknown("no renderer reads were recorded (renderer-map.jsonl)")
    if n.get("start") is None:
        return unknown("the load has no start")
    s0, s1 = n["start"], n["start"] + n["wall_ms"] / 1000
    slack = st.get("slack_s") or 0.0
    key = (n.get("session"), n.get("class"))
    ok = [m for m in r["reads"] if isinstance(m.get("pid"), int) and (m.get("session"), m.get("class")) == key]
    before = [m for m in ok if m["t1"] <= s0]
    after = [m for m in ok if m["t0"] >= s1]
    if not before or not after:
        return unknown("its renderer was not read both before and after it")
    b, a = max(before, key=lambda m: m["t1"]), min(after, key=lambda m: m["t0"])
    span = [m for m in ok if m["t0"] >= b["t0"] and m["t1"] <= a["t1"]]
    if len({m["pid"] for m in span}) != 1 or len({m.get("target") for m in span}) != 1:
        return unknown("its renderer changed while it loaded")
    if any(m.get("pid") is None and m.get("udd") == b.get("udd") and m["t0"] < a["t1"] and m["t1"] > b["t0"]
           for m in r["reads"]):
        return unknown("a read of its Chrome between the two failed")
    pid = b["pid"]
    k1, why1 = _series_of(r, pid, b["t0"], b["t1"])
    k2, why2 = _series_of(r, pid, a["t0"], a["t1"])
    if k1 is None or k2 is None or k1 != k2:
        return unknown(why1 or why2 or f"pid {pid} was another process at the read after the load")
    if r["udd"].get(k1) != b.get("udd"):
        return unknown("the pid Chrome named is not a renderer the sampler saw under that Chrome's --user-data-dir")
    s = r["series"][k1]
    if len(s) < 2 or max(v for _, v in s) == min(v for _, v in s):
        return unknown("its renderer's run_delay never advanced over the run: the counter is not shown live")
    lo = [i for i, (t, _) in enumerate(s) if t <= s0 - slack]
    hi = [i for i, (t, _) in enumerate(s) if t >= s1 + slack]
    if not lo or not hi:
        return unknown("no renderer sample brackets it")
    win = s[lo[-1]:hi[0] + 1]
    if r.get("per_thread"):
        at = (r.get("threads") or {}).get(k1) or {}
        if any(t not in at for t, _ in win):
            return unknown("a sample of its renderer that brackets it has no per-thread record")
        got, why = _thread_delta([t for t, _ in win], [at[t] for t, _ in win])
        if got is None:
            return unknown(why)
        ms = min(n["wall_ms"], got["ns"] / 1e6)
        return {"ms": ms, "bound_ms": got["bound_ms"], "upper_ms": min(n["wall_ms"], ms + got["bound_ms"]),
                "exited": got["exited"], "started": got["started"], "why": ""}
    d = win[-1][1] - win[0][1]
    if d < 0:
        return unknown("its renderer's summed run_delay decreased across it (a thread exited with its run_delay)")
    ms = min(n["wall_ms"], d / 1e6)
    return {"ms": ms, "bound_ms": 0.0, "upper_ms": ms, "exited": 0, "started": 0, "why": ""}


def possible_delay_ms(start: float, wall_ms: float, st: dict) -> float:
    """At most how much of a page load (from epoch `start`, `wall_ms` long)
    can have been the load generator waiting for a CPU: in each window it
    overlaps (widened by the anchor's measured error, st["slack_s"]), at
    most the window's stall, never more than the overlap itself; the WHOLE
    overlap where the node had no idle tick. Capped at the load's own time."""
    s0, s1 = start, start + wall_ms / 1000
    slack = st.get("slack_s") or 0.0
    total = 0.0
    for w in st["windows"]:
        overlap = min(s1, w["t1"] + slack) - max(s0, w["t0"] - slack)
        if overlap <= 0:
            continue
        total += overlap if w.get("saturated") else min(w.get("stall_s") or 0.0, overlap)
    return min(wall_ms, total * 1000)


def covered(start: float, wall_ms: float, st: dict) -> bool:
    """The load lies within the sampled span (its first window's start to
    its last window's end, widened by the anchor's error): only there is its
    stall measured (#1043 review B2)."""
    if not st["windows"]:
        return False
    slack = st.get("slack_s") or 0.0
    return st["windows"][0]["t0"] - slack <= start and start + wall_ms / 1000 <= st["windows"][-1]["t1"] + slack


#: the windows a container's cores are averaged over for their spread: long enough to hold several samples
#: (CPU_SAMPLE_S apart), short enough for an arm to hold several of them
SPREAD_WINDOW_S = 60.0


def _quantile(vals: list[float], q: float) -> float | None:
    """The q-quantile, interpolated, at the cores' own resolution (_pct rounds to 0.1: milliseconds')."""
    if not vals:
        return None
    v = sorted(vals)
    k = (len(v) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(v) - 1)
    return round(v[lo] + (v[hi] - v[lo]) * (k - lo), 4)


def container_cpu_report(recs: list[dict] | None) -> dict | None:
    """One container's own cgroup over the drive (k6_job._cpu_sampler's record): its mean cores (usage over the
    whole record), the p95 of its sample windows, its throttled time and its PSI some / full per second; and the
    RESOLUTION of that mean, from the arm's own spread: the record cut into SPREAD_WINDOW_S windows, the SD of
    their cores, and 2 SD / sqrt(n) (n the windows). Fewer than two windows: no spread, so no resolution (None,
    said). None when the record holds fewer than two samples."""
    if not recs:
        return None
    start = next((r for r in recs if r.get("kind") == "cpu_start"), {})
    pts = [r for r in recs if r.get("kind") == "cpu" and r.get("usage_usec") is not None]
    if len(pts) < 2 or float(pts[-1]["t"]) <= float(pts[0]["t"]):
        return None
    a, b = pts[0], pts[-1]
    span = float(b["t"]) - float(a["t"])

    def rate(k: str, x: dict, y: dict, scale: float) -> float | None:
        return (round((y[k] - x[k]) / scale / (float(y["t"]) - float(x["t"])), 4)
                if x.get(k) is not None and y.get(k) is not None else None)
    per = [rate("usage_usec", x, y, 1e6) for x, y in pairwise(pts) if float(y["t"]) > float(x["t"])]
    wins, w0 = [], a
    for p in pts[1:]:
        if float(p["t"]) - float(w0["t"]) >= SPREAD_WINDOW_S:
            wins.append(rate("usage_usec", w0, p, 1e6))
            w0 = p
    return {"request_m": start.get("request_m"), "limit_m": start.get("limit_m"), "seconds": round(span, 1),
            "cores_mean": rate("usage_usec", a, b, 1e6), "cores_p95": _quantile(per, 0.95),
            "throttled_s_per_s": rate("throttled_usec", a, b, 1e6),
            "psi_some_s_per_s": rate("pressure_some_usec", a, b, 1e6),
            "psi_full_s_per_s": rate("pressure_full_usec", a, b, 1e6),
            "spread": spread(wins)}


def spread(wins: list[float]) -> dict:
    """The resolution of a mean over SPREAD_WINDOW_S windows (#1525 review): load is correlated from one minute
    to the next, so the windows are not n independent samples. x the windows' cores, x-bar their mean:
    rho = sum_{i=1..n-1} (x_i - x-bar)(x_{i+1} - x-bar) / sum_{i=1..n} (x_i - x-bar)^2 (the mean-centred lag-1
    estimator over the full sum of squares, not a Pearson correlation of x[:-1] against x[1:]), clipped to
    [0, 0.99]; n_eff = n (1 - rho) / (1 + rho); SD the windows' sample SD (n - 1); resolution = 2 SD /
    sqrt(n_eff). n_eff under 2: unresolved, said so (no resolution stated)."""
    n = len(wins)
    out = {"window_s": SPREAD_WINDOW_S, "windows": n, "sd_cores": None, "rho": None, "n_eff": None,
           "resolution": None}
    if n < 2:
        return dict(out, why=f"{n} window(s) of {SPREAD_WINDOW_S:g} s: unresolved (no spread)")
    m = sum(wins) / n
    ss = sum((x - m) ** 2 for x in wins)
    sd = (ss / (n - 1)) ** 0.5
    raw = sum((x - m) * (y - m) for x, y in pairwise(wins)) / ss if ss > 0 else 0.0
    rho = min(0.99, max(0.0, raw))
    n_eff = n * (1 - rho) / (1 + rho)
    out.update(sd_cores=round(sd, 4), rho=round(rho, 4), n_eff=round(n_eff, 3))
    if n_eff < 2:
        return dict(out, why=f"{n} window(s) of {SPREAD_WINDOW_S:g} s with lag-1 autocorrelation {rho:.2f}: "
                             f"{n_eff:.2f} effective, under 2: unresolved")
    return dict(out, resolution=round(2 * sd / n_eff ** 0.5, 4))


#: the renderer watch a run was configured with (renderer_watch_of): on; off (a bare fixture's config: a measured
#: pod refuses it); absent (a recorded browser config from before #1517: no such flag); unreadable
WATCH_ON, WATCH_OFF, WATCH_ABSENT, WATCH_UNREADABLE = "on", "off", "absent", "unreadable"


def renderer_watch_of(manifest: str | None) -> tuple[str, str]:
    """(the renderer watch the run was CONFIGURED with, from its own recorded config, why) (#1525 review): the
    k6browser ConfigMap's browser-config.json in the run's loadgen-job.yaml, its `rendererWatch` (k6_job
    browser_config; render_browser refuses a pod without it on). WATCH_ABSENT when the config has no such key (a
    bench before #1517) or no manifest is given; WATCH_UNREADABLE when a manifest is given but its browser config
    cannot be read: what the run was configured to do is then unknown."""
    if manifest is None:
        return WATCH_ABSENT, "no loadgen manifest given"
    import yaml
    try:
        with open(manifest) as fh:
            docs = [d for d in yaml.safe_load_all(fh) if isinstance(d, dict)]
        cm = next(d for d in docs if d.get("kind") == "ConfigMap" and (d.get("metadata") or {}).get("name") == "k6browser")
        cfg = json.loads(cm["data"]["browser-config.json"])
    except (OSError, ValueError, StopIteration, KeyError, TypeError, yaml.YAMLError) as e:
        return WATCH_UNREADABLE, f"the run's browser config ({os.path.basename(manifest)}, ConfigMap k6browser) could " \
                                 f"not be read ({type(e).__name__})"
    if "rendererWatch" not in cfg:
        return WATCH_ABSENT, "the run's browser config states no renderer watch (a bench before #1517)"
    return (WATCH_ON if cfg["rendererWatch"] is True else WATCH_OFF), \
        f"the run's browser config: rendererWatch {cfg['rendererWatch']!r}"


def renderer_records_check(o: dict | None) -> dict | None:
    """loadgen.renderer_records (#1525 review): a run CONFIGURED with the renderer watch must hold every record
    the per-renderer rule reads: the renderer reads (renderer-map.jsonl, at least one read) and the renderer
    samples (browser-renderers.jsonl, at least one sample); each one missing or empty FAILS, named: INVALID
    (undecided), never judged as old evidence. A config that cannot be read FAILS too. None for a run whose
    recorded config states no watch (old evidence: the container rule)."""
    w = (o or {}).get("renderer_watch")
    base = {"check": "loadgen.renderer_records", "bar": "a run with the renderer watch holds its renderer records"}
    if w == WATCH_UNREADABLE:
        return dict(base, value=None, result="FAIL",
                    why="INVALID (undecided): " + str((o or {}).get("renderer_watch_why")) + ": whether the run had "
                        "the renderer watch, and so which rule judges its loads, is unknown")
    if w != WATCH_ON:
        return None
    missing = []
    if not any(m.get("kind") == "renderer_map" for m in o.get("renderer_map") or []):
        missing.append("renderer-map.jsonl (the renderer reads): " + ("absent" if o.get("renderer_map") is None
                                                                      else "no read"))
    if not any(r.get("kind") == "renderer" for r in o.get("renderers") or []):
        missing.append("browser-renderers.jsonl (the renderer samples): " + ("absent" if o.get("renderers") is None
                                                                            else "no sample"))
    if missing:
        return dict(base, value={"missing": missing}, result="FAIL",
                    why="INVALID (undecided): the run had the renderer watch and lacks " + "; ".join(missing)
                        + ": its loads' renderers are unknown, never judged as old evidence")
    return dict(base, value={"missing": []}, result="PASS")


def helper_cpu_check(o: dict | None) -> dict | None:
    """loadgen.renderers_cpu (Diego 2026-11-17: the helper's cost is measured, directly): a browser run CONFIGURED
    with the renderer watch (its recorded config, #1525 review: never inferred from which files exist) must hold
    the renderers sidecar's own CPU record with samples; missing or empty FAILS (a missing measure is never a zero
    cost). None for a run whose recorded config states no watch (a bench before #1517): no helper to measure."""
    if o is None or o.get("renderer_watch") != WATCH_ON:
        return None
    base = {"check": "loadgen.renderers_cpu", "bar": "the renderers sidecar's own CPU record, with samples"}
    recs = (o.get("cpu") or {}).get("renderers")
    rep = container_cpu_report(recs)
    if recs is None:
        return dict(base, value=None, result="FAIL",
                    why="INVALID (undecided): the renderers sidecar left no CPU record (renderers-cpu.jsonl): its "
                        "cost is unknown, never zero")
    if rep is None:
        return dict(base, value=None, result="FAIL",
                    why="INVALID (undecided): the renderers sidecar's CPU record holds fewer than two samples: its "
                        "cost is unknown, never zero")
    return dict(base, value=rep, result="PASS")


def cpu_check(o: dict | None, container: str, th: dict | None = None) -> dict:
    """The browser container's CPU p95 against K6_THRESHOLDS
    max_browser_cpu_p95_share_of_request: a REPORT line (the run's report,
    never its validity): usage above a reservation is not starvation."""
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
    per = Counter()
    for val, t in o["points"].get("route_table_mismatch", []):
        per[t.get("class") or "(no class tag)"] += int(val or 0)
    by = ", ".join(f"{n} of menu class {c}" for c, n in sorted(per.items()))
    out.append({"check": "k6.route_table", "value": mism, "bar": "== 0",
                "result": "PASS" if mism == 0 else "FAIL",
                **({} if mism == 0 else {"by_class": dict(per),
                                         "why": f"{mism} users' own menus differ from their menu "
                                                f"class's table (that class's preflight)"
                                                + (f": {by}" if by else "")
                                                + ": the portal did not show them the menu their "
                                                  "RBAC gives the class"})})
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
    # each load's start is its own t0 tag (epoch ms: the page's performance.timeOrigin + navT0, or
    # the driver's clock just before the action for a load that never settled): never the point's
    # time minus its wall time, which lands a QUIET_MS and more after the load (#1043 review B1)
    # #1508: a load past its bound is no longer censored at it: `completed` True (wall_ms is how long it really
    # took; `bound_ms` the bound it went past) or False (it never completed while the session watched it, up to
    # the drive's end or until the page's context went; wall_ms is how long it was watched, `cause` why it
    # ended). None: not recorded (a browser.js before #1508 stopped watching at the bound and wrote the bound)
    return [{"kind": "nav", "nav": t.get("method"), "page": t.get("route"),
             "session": t.get("session"), "class": t.get("class"), "wall_ms": round(v, 1),
             "visit": 1 if t.get("visit") == "first" else 2, "calls": 0, "errors": 0,
             "timeout": t.get("timeout") == "yes", "t0": t.get("t0"), "skeleton": t.get("skeleton"),
             "completed": {"yes": True, "no": False}.get(t.get("completed")),
             "bound_ms": int(t["bound"]) if str(t.get("bound") or "").isdigit() else None,
             "cause": t.get("cause"),
             **({"start": int(t["t0"]) / 1000} if str(t.get("t0") or "").isdigit() else {})}
            for v, t in o["points"].get("browser_page_load_ms", [])]


# ─── #1508: sign-in failures and sessions that ended early ──────────────────

SIGNIN_METHOD = "login"          # browser.js tags a class's sign-in arrival (its first load) method=login


def outcome_kind(n: dict) -> str:
    """A load past its bound: "completed late", "never completed" or "censored at its bound" (before #1508)."""
    return {True: "completed late", False: "never completed"}.get(n.get("completed"), "censored at its bound")


def load_outcome(n: dict) -> str:
    """How a load past its bound ended, in words: its real time, never completed, or censored (not known)."""
    w, b = n.get("wall_ms"), n.get("bound_ms")
    if n.get("completed") is True:
        return f"completed after {w:.0f} ms" + (f", {w - b:.0f} ms past its {b} ms bound" if b is not None else "")
    if n.get("completed") is False:
        return f"never completed (watched {w:.0f} ms)" + (f": {n['cause']}" if n.get("cause") else "")
    return (f"stopped at its bound ({w:.0f} ms): how long it really took was not recorded (a browser.js before "
            f"#1508 stopped watching there)")


def split_after_failed_signin(navs: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """(the page loads judged, the sign-in failures among them, the navigations dropped).

    A sign-in failure is a class's sign-in arrival (method login) past its bound. When it is not known to have
    completed (it never did, or a browser.js before #1508 stopped watching at the bound), the context never
    signed in: that class's later navigations (same session and identity class, from the sign-in's start on)
    are dropped, not page loads of a signed-in user and never page timeouts (run 27c N2: s2's class 2, 10
    navigations of an unsigned context, each 'timed out' by construction). browser.js from #1508 runs none."""
    failed: dict[tuple, float] = {}
    for n in navs:
        if n.get("nav") == SIGNIN_METHOD and n.get("timeout") and n.get("completed") is not True:
            k = (n.get("session"), n.get("class"))
            failed[k] = min(failed.get(k, float("inf")), n.get("start", float("-inf")))
    kept, dropped = [], []
    for n in navs:
        k = (n.get("session"), n.get("class"))
        if k in failed and n.get("nav") != SIGNIN_METHOD and n.get("start", float("inf")) >= failed[k]:
            dropped.append(n)
        else:
            kept.append(n)
    return kept, [n for n in kept if n.get("nav") == SIGNIN_METHOD and n.get("timeout")], dropped


def signin_failure_check(failures: list[dict], dropped: list[dict]) -> dict:
    """browser.signin_failures: each sign-in past its bound, once, with how it ended (FAIL: a user who could not
    sign in had the worst page load there is)."""
    out = {"check": "browser.signin_failures", "value": len(failures), "bar": "== 0",
           "result": "PASS" if not failures else "FAIL",
           "dropped_navigations": len(dropped)}
    if failures:
        out["why"] = ("a real-browser sign-in did not complete within its bound: "
                      + "; ".join(f"session {n.get('session')} class {n.get('class')}: {load_outcome(n)}"
                                  for n in failures)
                      + (f" ({len(dropped)} later navigation(s) of a context that never signed in were not page "
                         f"loads, and are not counted)" if dropped else ""))
    return out


_UNCAUGHT = re.compile(r'(?:time="([^"]+)" )?level=error msg="Uncaught \(in promise\) ((?:[^"\\]|\\.)*)"')
_SESSION_OF = re.compile(r"^(?:Error: )?session (\S+?): (.*)$", re.S)


def _log_ends(paths: list[str]) -> tuple[list[dict], str]:
    """(each uncaught error k6 logged for a browser iteration, as {session, cause}; "" or why unread)."""
    from bench.identity import redact_identity
    out = []
    for p in paths:
        try:
            with open(p, errors="replace") as fh:
                text = fh.read()
        except OSError as e:
            return [], f"the browser driver's log {os.path.basename(p)} could not be read ({e.strerror})"
        for m in _UNCAUGHT.finditer(text):
            msg = m.group(2).replace('\\"', '"').replace("\\n", "\n").replace("\\t", "\t").split("\n", 1)[0]
            s = _SESSION_OF.match(msg)
            at = epoch(m.group(1)) if m.group(1) else None
            # k6's log time is cut to the second: the end lies in [at, at + 1 s)
            out.append({"session": s.group(1) if s else None, "at": at, "at_until": None if at is None else at + 1.0,
                        "cause": redact_identity((s.group(2) if s else msg).strip())[:300]})
    return out, ""


def session_ends(o: dict | None, declared: int | None, logs: list[str] | None = None) -> dict:
    """#1508: which declared real-browser sessions ran to the drive's end, and each that ended early, with its
    cause. From browser.js's own browser_session_end points (from #1508: one per session, `early` and `cause`);
    for a stream without any (a browser.js before #1508), from the uncaught errors k6 logged for the browser's
    iterations (`logs`, driver-k6browser-*.log): an error naming no session is a session lost all the same.
    state UNKNOWN: neither (no evidence a session did not end early is ever 'none ended early')."""
    summ = (o or {}).get("summary") or {}
    names = summ.get("sessions") if isinstance(summ.get("sessions"), list) else None
    n_declared = declared if declared is not None else (len(names) if names is not None else None)
    out: dict = {"declared": n_declared, "names": names, "lost": [], "source": None, "state": "READ", "why": None}
    pts = ((o or {}).get("points") or {}).get("browser_session_end", [])
    if o is None:
        return dict(out, state="UNKNOWN", why="the browser left no output")
    if pts:
        out["source"] = "browser.js's browser_session_end points"
        ended, at = {}, {}
        times = ((o or {}).get("times") or {}).get("browser_session_end") or []
        for i, (_, t) in enumerate(pts):
            if t.get("session") not in ended:
                ended[t.get("session")] = t
                at[t.get("session")] = times[i] if i < len(times) else None
        for name in names or sorted(ended):
            t = ended.get(name)
            if t is None:
                out["lost"].append({"session": name, "at": None,
                                    "cause": "no end recorded: its iteration never finished (k6 stopped it, or the "
                                             "stream is cut)"})
            elif t.get("early") == "yes":
                out["lost"].append({"session": name, "cause": t.get("cause") or "not said",
                                    "class": t.get("class"), "at": at.get(name)})
        if names is None and n_declared is not None and len(ended) < n_declared:
            out["lost"].append({"session": None, "cause": f"{n_declared - len(ended)} declared session(s) recorded "
                                                          f"no end"})
        return out
    if not logs:
        return dict(out, state="UNKNOWN",
                    why="the browser recorded no session end (a browser.js before #1508) and no browser driver log "
                        "was given: whether a session ended early is unknown")
    ends, why = _log_ends(logs)
    if why:
        return dict(out, state="UNKNOWN", why=why)
    out["source"] = "the uncaught errors k6 logged (" + ", ".join(os.path.basename(p) for p in logs) + ")"
    out["lost"] = ends
    return out


def saturation_events(st: dict, w0: float, w1: float) -> tuple[list[dict] | None, str]:
    """#1517 (the owner's ruling, 2026-11-17: "saturation = environment, stated honestly"): the sample windows in
    which the load-generator node was MEASURED saturated (starvation()'s `saturated`: not one idle CPU tick on any
    CPU) that OVERLAP the session's window [w0, w1], i.e. share at least one instant with it (win.t0 <= w1 and
    win.t1 >= w0, each window widened by the anchor's error, st["slack_s"]). On a saturated node every running
    process waits for a CPU, the bench's own driver among them, so the bench cannot tell snowplow from its driver
    there: an environment event, said as such, with no claim of what it caused.
    (events, "") or (None, why) when it cannot be told: the node's CPU not measured, the samples not anchored, or
    a part of [w0, w1] that no sample window covers (an unsampled stretch is not "no saturation")."""
    if not st.get("node"):
        return None, "the loadgen node's CPU was not measured"
    if not st.get("anchored"):
        return None, "the CPU samples have no wall-clock anchor"
    slack = st.get("slack_s") or 0.0
    wins = sorted((w for w in st.get("windows") or [] if w["t0"] - slack <= w1 and w["t1"] + slack >= w0),
                  key=lambda w: w["t0"])
    hit = [w for w in wins if w.get("saturated")]
    if hit:
        return [{"t0": w["t0"], "t1": w["t1"],
                 "what": "load generator saturated (no idle CPU) during the session's window"} for w in hit], ""
    # no saturation seen: only a "no event" when the windows cover the whole session window, gap-free
    covered = bool(wins) and wins[0]["t0"] - slack <= w0 and wins[-1]["t1"] + slack >= w1 and all(
        b["t0"] - slack <= a["t1"] + slack for a, b in zip(wins, wins[1:]))
    if not covered:
        return None, "the loadgen node's CPU samples do not cover the whole session window"
    return [], ""


def sessions_check(ends: dict, loads: int | None = None, events: list[dict] | None = None,
                   events_why: str = "", last_activity: dict | None = None,
                   drive_start: float | None = None, saturation=None) -> tuple[dict, dict | None]:
    """(environment.browser_sessions, browser.sessions_lost): every declared session ran to the drive's end; one
    that did not is judged by CAUSE (the owner's ruling, 2026-11-17), from evidence the bench records only:

      - a lost session whose last stretch (its last recorded activity, `last_activity`, any class's last load, else
        the drive's start, `drive_start`; to the latest its end can be, `at_until`, else `at`) a recorded
        environment event overlaps, i.e. shares at least one instant with (event t0 <= window end and event t1 >=
        window start; `events`: each {t0, t1, what}: a control-plane operation of the cluster's cluster-operations
        record), is INVALID by environment, named with the event;
      - one that no recorded event overlaps is a FAIL (a product failure: browser.sessions_lost), named with its
        cause and "no environment event in its window";
      - one whose end is not recorded, or a run whose events could not be read (`events` None: `events_why`), is
        INVALID (undecided): its cause cannot be told.
    Evidence without per-renderer data has only the control-plane operations as environment events; with it
    (#1517), `saturation(w0, w1)` -> (events, "") or (None, why unmeasured) adds the load generator's node measured
    saturated in a sample window overlapping the session's window (saturation_events), and a node it cannot tell
    for that window leaves the loss undecided."""
    base = {"check": "environment.browser_sessions", "bar": "every declared browser session runs to the drive's end"}
    if ends.get("state") != "READ":
        return dict(base, value=None, result="FAIL",
                    why="INVALID by environment (undecided): " + str(ends.get("why"))), None
    lost = ends.get("lost") or []
    if not lost:
        return dict(base, value=0, result="PASS", source=ends.get("source")), None
    n = ends.get("declared")
    env, product, undecided = [], [], []
    for x in lost:
        at = x.get("at")
        if at is None:
            undecided.append(dict(x, why="its end is not recorded"))
            continue
        if events is None:
            undecided.append(dict(x, why=f"the environment's events could not be read ({events_why})"))
            continue
        # #1520 review: the window opens at the session's last recorded activity (any class's last load: a fresh
        # class lost at its sign-in opens at the class before it), else at the drive's start, never at the loss
        # itself; it closes at the latest the end could be (`at_until`: k6's log times are cut to the second)
        w0 = (last_activity or {}).get(x.get("session"), drive_start)
        if w0 is None:
            undecided.append(dict(x, why="no activity of it and no start of the drive is recorded: its window is "
                                         "not known"))
            continue
        w0, w1 = min(w0, at), max(at, x.get("at_until") or at)
        hit = [e for e in events if e["t0"] <= w1 and (e["t1"] is None or e["t1"] >= w0)]
        if not hit and saturation is not None:
            # #1517 ("saturation = environment, stated honestly"): the load generator measured saturated in a window
            # overlapping the session's is an environment event too; one it cannot tell leaves the loss undecided
            got, why = saturation(w0, w1)
            if got is None:
                undecided.append(dict(x, window=[w0, w1], why=why))
                continue
            hit = got
        (env if hit else product).append(dict(x, window=[w0, w1], events=[e["what"] for e in hit]))
    tail = ((f": the arm drove {max(0, n - len(lost))} of {n} sessions to the end" if n else "")
            + (f" ({loads} page loads)" if loads is not None else ""))
    named = lambda xs: "; ".join(f"{x.get('session') or '(session not named)'}: {x.get('cause')}" for x in xs)  # noqa: E731
    product_check = None
    if product:
        product_check = {"check": "browser.sessions_lost", "value": len(product), "bar": "== 0", "result": "FAIL",
                         "lost": product,
                         "why": (f"{len(product)} real-browser session(s) ended before the drive's end with no "
                                 f"environment event in their window (a product failure): "
                                 + "; ".join(f"{x.get('session') or '(session not named)'}: {x.get('cause')}, no "
                                             f"environment event in its window" for x in product) + tail)}
    if env or undecided:
        parts = []
        if env:
            parts.append(f"INVALID by environment: {len(env)}" + (f" of {n}" if n else "")
                         + " declared real-browser session(s) ended before the drive's end while a recorded "
                           "environment event overlapped them: "
                         + "; ".join(f"{x.get('session') or '(session not named)'}: {x.get('cause')} (during "
                                     f"{'; '.join(x['events'])})" for x in env))
        if undecided:
            parts.append("INVALID by environment (undecided): " + "; ".join(
                f"{x.get('session') or '(session not named)'}: {x.get('cause')} ({x['why']})" for x in undecided))
        return dict(base, value=len(env) + len(undecided), result="FAIL", lost=env + undecided,
                    source=ends.get("source"), why="; ".join(parts) + tail), product_check
    return dict(base, value=0, result="PASS", source=ends.get("source"),
                note=f"{len(product)} lost session(s) judged a product failure (browser.sessions_lost)"), product_check


BURST_KINDS = ("first_load", "composite_first_load")


def k6_burst_navs(o: dict | None) -> list[dict]:
    """k6's first loads and composite deep links (navigate.js tags each
    page_load_ms with its kind) as the nav records run8_report's burst
    windows read: {"nav": kind, "start", "wall_ms"}. The browser's navs carry
    the load's method instead, never these kinds."""
    return [{"kind": "nav", "nav": t.get("kind"), "start": s, "wall_ms": ms, "page": t.get("route")}
            for s, ms, t in k6_page_loads(o) if t.get("kind") in BURST_KINDS]


def widget_timeouts_by_load(o: dict | None) -> tuple[dict, int]:
    """({(session, t0): widget-timeout states}, how many such states carry no
    t0): which load each widget-timeout state belongs to."""
    out, unplaced = {}, 0
    for v, t in ((o or {}).get("points") or {}).get("browser_widget_states", []):
        if t.get("state") != "timeout":
            continue
        if not str(t.get("t0") or "").isdigit():
            unplaced += 1
            continue
        k = (t.get("session"), t["t0"])
        out[k] = out.get(k, 0) + 1
    return out, unplaced


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
    from bench import calltiming
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
            "calls_report_only": calls_report(p.get("browser_call_ms", [])),
            # #1089: each /call's protocol, connection queueing and waterfall
            "call_timing_report_only": calltiming.report(o),
            "incomplete_requests_report_only": incomplete_report(p.get("browser_request_incomplete_ms", []),
                                                                 p.get("browser_request_failed_start_unseen", [])),
            "cpu": cpu_stats((o.get("cpu") or {}).get("browser"))}


CALL_KEY = ("call_resource", "call_namespace", "call_name")


def incomplete_report(points: list, unseen: list | None = None) -> dict:
    """#919 (N7): the browser requests that failed at network level or never
    completed (browser.js browser_request_incomplete_ms; k6 itself emits
    nothing for them): how many, by outcome / request kind / answered / error,
    and per /call; each one's ms ran from its start to its failure or the
    session's end. Failed requests whose start was never seen have no
    duration: counted apart (start_unseen). A browser stream without the
    metric says so: missing, not zero."""
    if not points:
        return {"status": "absent", "note": "no browser_request_incomplete_ms point: MISSING (browser.js "
                                            "before #919, or no session ran), not zero — every session "
                                            "ends with its live streams still open"}
    kinds: dict[str, int] = defaultdict(int)
    for _, t in points:
        kinds[f"{t.get('outcome')} {t.get('request')} answered={t.get('answered')} {t.get('error') or '-'}"] += 1
    gone: dict[str, int] = defaultdict(int)
    for v, t in unseen or []:
        gone[f"{t.get('request')} {t.get('error') or '-'}"] += int(v or 0)
    return {"status": "present", "requests": len(points),
            "by_outcome": {o: sum(1 for _, t in points if t.get("outcome") == o)
                           for o in sorted({t.get("outcome") for _, t in points})},
            "by_outcome_request_answered_error": dict(sorted(kinds.items())),
            "calls": _by([pt for pt in points if pt[1].get("request") == "call"], "outcome", *CALL_KEY),
            "failed_start_unseen": dict(sorted(gone.items()))}


def calls_report(points: list) -> dict:
    """#919: the browsers' /call requests (browser.js browser_call_ms: each
    one's Resource Timing duration), by the /call's resource / namespace /
    name, by the page route whose load made them, and by HTTP status. A
    browser stream without one browser_call_ms point says so: missing, not
    zero (a browser.js before #919)."""
    if not points:
        return {"status": "absent", "note": "no browser_call_ms point: the per-/call times are missing "
                                            "(browser.js before #919), not zero"}
    status: dict[str, int] = defaultdict(int)
    for _, t in points:
        status["/".join(str(t.get(k)) for k in CALL_KEY) + f" {t.get('call_status')}"] += 1
    return {"status": "present", "calls": len(points),
            "by_call": _by(points, *CALL_KEY),
            # a page load's own calls (during=load), by the route that loaded
            "by_route_call": _by([pt for pt in points if pt[1].get("during") == "load"], "route", *CALL_KEY),
            "by_call_status": dict(sorted(status.items()))}


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
