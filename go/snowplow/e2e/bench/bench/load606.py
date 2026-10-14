"""#606 production-scale load driver, freshness probe, expvar sampler, verdict.

STDLIB ONLY and no `bench.*` imports: the file is shipped as a ConfigMap into
a python:3 pod on the bench cluster's `loadgen` pool, so 1000 virtual users
never share a node (or a laptop uplink) with what they measure.

What it does
------------
drive    N virtual users replay the portal SPA over HTTP: GET a page widget,
         follow every GET+allowed `status.resourcesRefs.items[].path`, level
         by level, `per_user_parallel` at a time (a browser's per-host limit),
         until the tree is exhausted. One NAVIGATION = one page walk; its wall
         time is first request start → last response end. Users think for an
         exponential delay between navigations (open loop: the schedule does
         not slow down when snowplow does — scheduling lag is recorded and a
         lagging driver INVALIDATES the run rather than flattering it).
fresh    Mutate a probe object (caller-supplied `mutate(marker)`, kubectl in
         practice) and poll a /call path every `poll_ms` until the decoded body
         contains the marker. Latency = patch-returned → marker visible.
churn    Background event generator: `rate` mutations/s spread over many
         compositions, the independent variable of the #603 fan-out law.
sample   GET /debug/vars every `interval` s, keep the refresh-economics
         counters (refresher, dirty-mark attribution, resolved cache).
evaluate Turn the JSONL outputs into the north-star verdict (see THRESHOLDS).

Honesty rules baked in (see feedback memories cited in the run plan):
  - the READ denominator is the driver's own request count, never a server
    counter that correlates with reads (#552 contaminates hit_total);
  - a counter that is absent reads as None and fails the verdict closed;
  - a driver that could not keep its schedule marks the run INVALID.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import gzip
import json
import math
import os
import random
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field, asdict
from typing import Callable, Iterable

__all__ = [
    "THRESHOLDS",
    "HttpClient",
    "extract_refs",
    "walk_page",
    "Driver",
    "freshness_probe",
    "ChurnDriver",
    "summarize_vars",
    "economics",
    "pct",
    "evaluate",
    "list_dep_fixture_yaml",
    "call_path",
    "main",
]

# North-star bars (project_north_star: 1s cold / 500ms warm / 1s fresh; #506
# ruling: zero cold navigations is a REQUIREMENT). The driver's own validity
# bars are below them.
# The synthetic scale users' fixed TEST password (throwaway users on a
# throwaway cluster). Single definition: lifecycle._scaleuser_password() and
# run606.sh read it from here, and run606.sh redacts it from every log line.
SCALEUSER_PASSWORD = "ScaleTest2026!"

THRESHOLDS = {
    "warm_nav_p95_ms": 500,
    "cold_nav_p95_ms": 1000,
    "fresh_p95_ms": 1000,
    # Zero cold navigations, measured from the client because snowplow sends
    # no per-response cache-outcome signal: every FIRST navigation of a
    # (user, page) must itself meet the WARM bar, and no navigation at all may
    # exceed the cold bar.
    "first_nav_p95_ms_for_zero_cold": 500,
    "max_nav_ms_for_zero_cold": 1000,
    # Driver validity.
    "max_sched_lag_p95_ms": 1000,
    "max_error_rate": 0.01,
    "min_fresh_samples": 30,
    "min_warm_navs": 200,
}


def pct(values: Iterable[float], p: float) -> float | None:
    """Nearest-rank percentile; None on empty input."""
    xs = sorted(v for v in values if v is not None)
    if not xs:
        return None
    k = max(0, min(len(xs) - 1, math.ceil(p / 100.0 * len(xs)) - 1))
    return xs[k]


# ─── HTTP ───────────────────────────────────────────────────────────────────


def _decode(body: bytes, headers) -> bytes:
    enc = (headers.get("Content-Encoding") or "").lower() if headers else ""
    if enc == "gzip" or body[:2] == b"\x1f\x8b":
        try:
            return gzip.decompress(body)
        except OSError:
            return body
    return body


@dataclass
class Response:
    status: int
    body: bytes
    ms: float
    error: str = ""


class HttpClient:
    def __init__(self, base_url: str, timeout: float = 30.0):
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def get(self, path: str, token: str | None) -> Response:
        headers = {"Accept-Encoding": "gzip"}
        if token:
            headers["Authorization"] = "Bearer " + token
        req = urllib.request.Request(self.base + path, headers=headers)
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                raw = r.read()
                return Response(r.status, _decode(raw, r.headers),
                                (time.perf_counter() - t0) * 1000)
        except urllib.error.HTTPError as e:
            return Response(e.code, b"", (time.perf_counter() - t0) * 1000,
                            f"http {e.code}")
        except Exception as e:  # noqa: BLE001 — every failure is a sample
            return Response(0, b"", (time.perf_counter() - t0) * 1000,
                            f"{type(e).__name__}: {e}")


def login(authn_url: str, user: str, password: str, timeout: float = 30) -> str:
    creds = base64.b64encode(f"{user}:{password}".encode()).decode()
    req = urllib.request.Request(authn_url.rstrip("/") + "/basic/login",
                                 headers={"Authorization": "Basic " + creds})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)["accessToken"]


# ─── SPA walk ───────────────────────────────────────────────────────────────


def extract_refs(doc) -> list[str]:
    """Paths the SPA would fetch next from one resolved widget.

    GET + allowed refs under status.resourcesRefs.items whose path is a
    snowplow /call. A ref already embedded server-side (`rendered`, #72
    inline children) is NOT fetched — the browser does not fetch it either.
    """
    if not isinstance(doc, dict):
        return []
    items = (((doc.get("status") or {}).get("resourcesRefs") or {})
             .get("items") or [])
    out = []
    for it in items:
        if not isinstance(it, dict):
            continue
        if (it.get("verb") or "GET").upper() != "GET" or not it.get("allowed"):
            continue
        if it.get("rendered"):
            continue
        p = it.get("path") or ""
        if p.startswith("/call"):
            out.append(p)
    return out


@dataclass
class NavResult:
    user: str
    page: str
    visit: int            # 1 = this user's first visit of this page
    start: float          # epoch seconds
    wall_ms: float
    calls: int
    errors: int
    bytes: int
    slowest_call_ms: float
    sched_lag_ms: float = 0.0
    error_samples: list = field(default_factory=list)


def walk_page(client: HttpClient, token: str, root: str, *,
              pool: concurrent.futures.Executor, per_user_parallel: int = 6,
              max_depth: int = 8, max_calls: int = 2000):
    """Walk one page tree. Returns (wall_ms, calls, errors, bytes, slowest,
    error_samples)."""
    seen = {root}
    level = [root]
    calls = errors = nbytes = 0
    slowest = 0.0
    samples: list[str] = []
    t0 = time.perf_counter()
    depth = 0
    while level and depth <= max_depth and calls < max_calls:
        nxt: list[str] = []
        for i in range(0, len(level), per_user_parallel):
            chunk = level[i:i + per_user_parallel]
            for path, resp in zip(chunk, pool.map(
                    lambda p: client.get(p, token), chunk)):
                calls += 1
                slowest = max(slowest, resp.ms)
                nbytes += len(resp.body)
                if resp.status != 200:
                    errors += 1
                    if len(samples) < 5:
                        samples.append(f"{resp.status} {path[:160]} {resp.error}")
                    continue
                try:
                    doc = json.loads(resp.body or b"null")
                except ValueError:
                    continue
                for ref in extract_refs(doc):
                    if ref not in seen:
                        seen.add(ref)
                        nxt.append(ref)
        level = nxt
        depth += 1
    wall = (time.perf_counter() - t0) * 1000
    return wall, calls, errors, nbytes, slowest, samples


# ─── Driver ─────────────────────────────────────────────────────────────────


class Driver:
    """Open-loop virtual-user scheduler.

    `pages` is [(name, root_path, weight)]. Each user's next navigation time
    is drawn from an exponential think time; the scheduler dispatches due
    navigations into a bounded nav pool and records how late each one started.
    """

    def __init__(self, client: HttpClient, tokens: dict[str, str],
                 pages: list[tuple[str, str, float]], *,
                 think_s: float = 60.0, duration_s: float = 1800.0,
                 nav_workers: int = 128, call_workers: int = 512,
                 per_user_parallel: int = 6, seed: int = 606,
                 sink: Callable[[dict], None] | None = None,
                 clock: Callable[[], float] = time.time,
                 sleep: Callable[[float], None] = time.sleep):
        if not pages:
            raise ValueError("no pages to navigate")
        self.client = client
        self.tokens = tokens
        self.pages = pages
        self.think_s = think_s
        self.duration_s = duration_s
        self.nav_pool = concurrent.futures.ThreadPoolExecutor(nav_workers)
        self.call_pool = concurrent.futures.ThreadPoolExecutor(call_workers)
        self.per_user_parallel = per_user_parallel
        self.rng = random.Random(seed)
        self.sink = sink or (lambda rec: None)
        self.clock = clock
        self.sleep = sleep
        self.visits: dict[tuple[str, str], int] = {}
        self.lock = threading.Lock()
        self.results: list[NavResult] = []

    def _pick_page(self):
        total = sum(w for _, _, w in self.pages)
        x = self.rng.uniform(0, total)
        for name, root, w in self.pages:
            x -= w
            if x <= 0:
                return name, root
        return self.pages[-1][0], self.pages[-1][1]

    def _nav(self, user: str, page: str, root: str, due: float) -> None:
        started = self.clock()
        with self.lock:
            visit = self.visits.get((user, page), 0) + 1
            self.visits[(user, page)] = visit
        wall, calls, errors, nbytes, slowest, samples = walk_page(
            self.client, self.tokens[user], root, pool=self.call_pool,
            per_user_parallel=self.per_user_parallel)
        res = NavResult(user=user, page=page, visit=visit, start=started,
                        wall_ms=round(wall, 1), calls=calls, errors=errors,
                        bytes=nbytes, slowest_call_ms=round(slowest, 1),
                        sched_lag_ms=round(max(0.0, started - due) * 1000, 1),
                        error_samples=samples)
        with self.lock:
            self.results.append(res)
        self.sink({"kind": "nav", **asdict(res)})

    def run(self) -> list[NavResult]:
        t_start = self.clock()
        end = t_start + self.duration_s
        # First navigation of every user is spread over one think period so
        # 1000 users do not all log in at the same instant.
        nxt = {u: t_start + self.rng.uniform(0, self.think_s)
               for u in self.tokens}
        futures = []
        while True:
            now = self.clock()
            if now >= end:
                break
            due = [(t, u) for u, t in nxt.items() if t <= now]
            for t, u in due:
                page, root = self._pick_page()
                futures.append(self.nav_pool.submit(self._nav, u, page, root, t))
                nxt[u] = t + self.rng.expovariate(1.0 / self.think_s)
            soonest = min(nxt.values())
            self.sleep(max(0.0, min(0.05, soonest - self.clock())))
        for f in futures:
            f.result()
        self.nav_pool.shutdown(wait=True)
        self.call_pool.shutdown(wait=True)
        return self.results


# ─── Freshness ──────────────────────────────────────────────────────────────


def freshness_probe(client: HttpClient, token: str, path: str,
                    mutate: Callable[[str], bool], *, samples: int = 60,
                    poll_ms: int = 100, budget_s: float = 30.0,
                    gap_s: float = 5.0, label: str = "list_dep",
                    sink: Callable[[dict], None] | None = None,
                    clock: Callable[[], float] = time.time,
                    sleep: Callable[[float], None] = time.sleep) -> list[dict]:
    """Mutate → poll until the marker is visible. One record per sample.

    The clock starts when `mutate` RETURNS (the apiserver has committed the
    write), so the latency is informer → dirty-mark → refresh → serve, not the
    driver's own network hop. A sample that never converges records
    latency_ms=None and converged=False; the verdict fails on it (it is not
    dropped). The poll interval bounds resolution: report it with the result.
    """
    sink = sink or (lambda rec: None)
    out = []
    for i in range(samples):
        marker = f"fresh-{label}-{int(clock() * 1000)}-{i}"
        if not mutate(marker):
            rec = {"kind": "fresh", "label": label, "marker": marker,
                   "converged": False, "latency_ms": None, "polls": 0,
                   "error": "mutate failed", "poll_ms": poll_ms}
            out.append(rec)
            sink(rec)
            continue
        t0 = clock()
        polls = 0
        lat = None
        while clock() - t0 < budget_s:
            polls += 1
            r = client.get(path, token)
            if r.status == 200 and marker.encode() in r.body:
                lat = round((clock() - t0) * 1000, 1)
                break
            sleep(poll_ms / 1000.0)
        rec = {"kind": "fresh", "label": label, "marker": marker,
               "converged": lat is not None, "latency_ms": lat,
               "polls": polls, "poll_ms": poll_ms, "t": t0}
        out.append(rec)
        sink(rec)
        sleep(gap_s)
    return out


class ChurnDriver:
    """`rate` mutations/s over `targets`, round-robin, in a background thread.

    `mutate(target, marker) -> bool`. Records how many it issued and how many
    failed, so the verdict can state the event rate it actually achieved.
    """

    def __init__(self, targets: list, rate: float,
                 mutate: Callable[[object, str], bool], workers: int = 16):
        if rate <= 0 or not targets:
            raise ValueError("rate must be > 0 and targets non-empty")
        self.targets = targets
        self.rate = rate
        self.mutate = mutate
        self.pool = concurrent.futures.ThreadPoolExecutor(workers)
        self.stop_evt = threading.Event()
        self.issued = 0
        self.failed = 0
        self.lock = threading.Lock()
        self.t0 = 0.0
        self.t1 = 0.0
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def _one(self, target, marker):
        ok = False
        try:
            ok = bool(self.mutate(target, marker))
        finally:
            with self.lock:
                self.issued += 1
                if not ok:
                    self.failed += 1

    def _loop(self):
        self.t0 = time.time()
        i = 0
        while not self.stop_evt.is_set():
            target = self.targets[i % len(self.targets)]
            self.pool.submit(self._one, target, f"churn-{i}")
            i += 1
            due = self.t0 + i / self.rate
            self.stop_evt.wait(max(0.0, due - time.time()))
        self.t1 = time.time()

    def start(self):
        self.thread.start()
        return self

    def stop(self) -> dict:
        self.stop_evt.set()
        self.thread.join()
        self.pool.shutdown(wait=True)
        span = max(1e-9, self.t1 - self.t0)
        return {"issued": self.issued, "failed": self.failed,
                "target_rate": self.rate,
                "achieved_rate": round((self.issued - self.failed) / span, 2)}


# ─── /debug/vars ────────────────────────────────────────────────────────────


def _sum_l1(lookups) -> tuple[int | None, int | None]:
    if not isinstance(lookups, dict):
        return None, None
    hit = miss = 0
    for cell in lookups.values():
        if isinstance(cell, dict):
            hit += int(cell.get("hit_total", 0))
            miss += int(cell.get("miss_total", 0))
    return hit, miss


def summarize_vars(v: dict) -> dict:
    """Keep the refresh-economics fields of one /debug/vars document.

    Absent keys stay None (never 0): an absent counter is not a zero reading.
    """
    def g(k):
        return v.get(k)

    attr = g("snowplow_deps_dirty_mark_attribution") or {}
    by_class: dict[str, int] = {}
    for b in attr.get("by_bucket") or []:
        if isinstance(b, dict):
            by_class[b.get("class", "?")] = (by_class.get(b.get("class", "?"), 0)
                                             + int(b.get("count", 0)))
    rc = g("snowplow_resolved_cache") or {}
    l1_hit, l1_miss = _sum_l1(g("snowplow_dispatch_l1_lookups"))
    mem = g("memstats") or {}
    return {
        "refresher_enqueue": g("snowplow_refresher_enqueue_total"),
        "refresher_completed": g("snowplow_refresher_completed_total"),
        "refresher_dropped": g("snowplow_refresher_dropped_total"),
        "refresher_failed": g("snowplow_refresher_failed_total"),
        "refresher_queue_depth": g("snowplow_refresher_queue_depth"),
        "dirty_to_fresh_p95_ms": g("snowplow_refresher_dirty_to_fresh_ms_p95"),
        "dirty_to_fresh_max_ms": g("snowplow_refresher_dirty_to_fresh_ms_max"),
        "dirty_to_fresh_samples": g("snowplow_refresher_dirty_to_fresh_samples_total"),
        "marks_by_class": by_class if attr else None,
        "mark_events": attr.get("events_total") if attr else None,
        "marks_unattributed": attr.get("unattributed") if attr else None,
        "cache_entries": rc.get("entries"),
        "cache_bytes": rc.get("bytes"),
        "cache_hit": rc.get("hit_total"),
        "cache_miss": rc.get("miss_total"),
        "l1_hit": l1_hit,
        "l1_miss": l1_miss,
        "prewarm_complete": g("snowplow_prewarm_complete"),
        "heap_inuse": mem.get("HeapInuse"),
        "num_gc": mem.get("NumGC"),
    }


def read_vars(client: HttpClient, token: str) -> dict | None:
    r = client.get("/debug/vars", token)
    if r.status != 200:
        return None
    try:
        return json.loads(r.body)
    except ValueError:
        return None


def _delta(a, b):
    if a is None or b is None:
        return None
    return b - a


def economics(first: dict, last: dict, customer_calls: int) -> dict:
    """Refresh economics over [first, last] (two summarize_vars outputs).

    completions_per_read uses the DRIVER's request count as the denominator.
    """
    completed = _delta(first.get("refresher_completed"),
                       last.get("refresher_completed"))
    enq = _delta(first.get("refresher_enqueue"), last.get("refresher_enqueue"))
    m0 = first.get("marks_by_class") or None
    m1 = last.get("marks_by_class") or None
    marks = list_dep = None
    if m0 is not None and m1 is not None:
        marks = sum(m1.values()) - sum(m0.values())
        list_dep = m1.get("list_dep", 0) - m0.get("list_dep", 0)
    events = _delta(first.get("mark_events"), last.get("mark_events"))
    return {
        "customer_calls": customer_calls,
        "refresher_completed": completed,
        "refresher_enqueued": enq,
        "refresher_dropped": _delta(first.get("refresher_dropped"),
                                    last.get("refresher_dropped")),
        "completions_per_read": (round(completed / customer_calls, 3)
                                 if completed is not None and customer_calls
                                 else None),
        "dirty_marks": marks,
        "list_dep_marks": list_dep,
        "list_dep_share": (round(list_dep / marks, 4)
                           if marks else None),
        "mark_events": events,
        "marks_per_event": (round(marks / events, 2)
                            if marks is not None and events else None),
    }


# ─── Verdict ────────────────────────────────────────────────────────────────


def _check(name, value, op, bar, checks):
    if value is None:
        checks.append({"check": name, "value": None, "bar": bar,
                       "result": "FAIL", "why": "no data"})
        return
    ok = value <= bar if op == "<=" else value >= bar
    checks.append({"check": name, "value": value, "bar": f"{op} {bar}",
                   "result": "PASS" if ok else "FAIL"})


def evaluate(navs: list[dict], fresh: list[dict], vars_series: list[dict],
             *, thresholds: dict | None = None) -> dict:
    """North-star verdict from the run's JSONL records.

    Returns {"verdict": PASS|FAIL|INVALID, "checks": [...], "economics": ...}.
    INVALID (the driver, not snowplow, failed) wins over FAIL.
    """
    th = dict(THRESHOLDS, **(thresholds or {}))
    checks: list[dict] = []
    validity: list[dict] = []

    first = [n["wall_ms"] for n in navs if n.get("visit") == 1]
    warm = [n["wall_ms"] for n in navs if (n.get("visit") or 0) > 1]
    calls = sum(int(n.get("calls", 0)) for n in navs)
    errors = sum(int(n.get("errors", 0)) for n in navs)

    _check("warm_nav_p95_ms", pct(warm, 95), "<=", th["warm_nav_p95_ms"], checks)
    _check("cold_nav_p95_ms", pct(first, 95), "<=", th["cold_nav_p95_ms"], checks)
    _check("zero_cold_navs.first_nav_p95_ms", pct(first, 95), "<=",
           th["first_nav_p95_ms_for_zero_cold"], checks)
    over = sum(1 for n in navs if n["wall_ms"] > th["max_nav_ms_for_zero_cold"])
    _check("zero_cold_navs.navs_over_cold_bar", over if navs else None, "<=", 0,
           checks)

    # Ruling 2026-10-14: freshness GATES at the 10 events/s churn step; the
    # other rates (1, 50) are REPORTED as the curve. Labels are
    # "list_dep@<rate>"; a label without "@" (single-rate run) gates.
    gate_labels = set(th.get("fresh_gate_labels", ("list_dep@10",)))
    curve = {}
    labels = sorted({f.get("label", "?") for f in fresh})
    rated = [l for l in labels if "@" in l]
    if rated and not (gate_labels & set(rated)):
        checks.append({"check": f"fresh_p95_ms[{'/'.join(sorted(gate_labels))}]",
                       "value": None, "bar": f"<= {th['fresh_p95_ms']}",
                       "result": "FAIL", "why": "no samples at the gate churn rate"})
    for label in labels:
        rows = [f for f in fresh if f.get("label") == label]
        lat = [f["latency_ms"] for f in rows if f.get("converged")]
        unconverged = sum(1 for f in rows if not f.get("converged"))
        if "@" in label and label not in gate_labels:
            curve[label] = {"samples": len(rows), "unconverged": unconverged,
                            "p50_ms": pct(lat, 50), "p95_ms": pct(lat, 95),
                            "max_ms": max(lat) if lat else None}
            continue
        if unconverged:
            # A sample that never converged is the worst latency there is; it
            # fails the bar rather than leaving the percentile.
            checks.append({"check": f"fresh_p95_ms[{label}]", "value": None,
                           "bar": f"<= {th['fresh_p95_ms']}", "result": "FAIL",
                           "why": f"{unconverged}/{len(rows)} samples never "
                                  f"converged"})
        else:
            _check(f"fresh_p95_ms[{label}]", pct(lat, 95), "<=",
                   th["fresh_p95_ms"], checks)
        _check(f"fresh_samples[{label}]", len(rows), ">=",
               th["min_fresh_samples"], validity)
    if not fresh:
        _check("fresh_p95_ms", None, "<=", th["fresh_p95_ms"], checks)

    _check("driver.sched_lag_p95_ms",
           pct([n.get("sched_lag_ms", 0) for n in navs], 95), "<=",
           th["max_sched_lag_p95_ms"], validity)
    _check("driver.error_rate", round(errors / calls, 4) if calls else None,
           "<=", th["max_error_rate"], validity)
    _check("driver.warm_navs", len(warm), ">=", th["min_warm_navs"], validity)

    econ = None
    if len(vars_series) >= 2:
        econ = economics(vars_series[0], vars_series[-1], calls)
    q = [v.get("refresher_queue_depth") for v in vars_series
         if v.get("refresher_queue_depth") is not None]
    p95s = [v.get("dirty_to_fresh_p95_ms") for v in vars_series
            if v.get("dirty_to_fresh_p95_ms") is not None]
    report = {
        "navs": len(navs), "first_navs": len(first), "warm_navs": len(warm),
        "calls": calls, "errors": errors,
        "warm_p50_ms": pct(warm, 50), "warm_p95_ms": pct(warm, 95),
        "first_p50_ms": pct(first, 50), "first_p95_ms": pct(first, 95),
        "max_nav_ms": max((n["wall_ms"] for n in navs), default=None),
        "refresher_queue_depth_max": max(q) if q else None,
        "refresher_queue_depth_last": q[-1] if q else None,
        "server_dirty_to_fresh_p95_ms_max_window": max(p95s) if p95s else None,
        "server_dirty_to_fresh_p95_ms_median_window": pct(p95s, 50),
        "fresh_curve_report_only": curve,
    }

    if any(c["result"] == "FAIL" for c in validity):
        verdict = "INVALID"
    elif any(c["result"] == "FAIL" for c in checks):
        verdict = "FAIL"
    else:
        verdict = "PASS"
    return {"verdict": verdict, "checks": checks, "validity": validity,
            "report": report, "economics": econ, "thresholds": th}


# ─── Fixtures + kubectl mutation (operator side) ────────────────────────────

FRESH_ANNOTATION = "snowplow-bench/fresh-marker"
FRESH_RA_NAME = "bench-fresh-listdep"


def list_dep_fixture_yaml(ns: str, group: str, version: str, plural: str,
                          ra_ns: str = "bench-ns-01") -> str:
    """A RESTAction whose only stage LISTs one namespace of compositions and
    projects each item's freshness annotation.

    Served through the informer, so an annotation patch on ANY composition in
    `ns` reaches this cell ONLY through a list_dep edge (keyed on GVR + ns) —
    the exact edge #603 narrows. No query string: #618, the informer LIST
    ignores ?labelSelector/limit and would serve the unselected list.
    """
    path = f"/apis/{group}/{version}/namespaces/{ns}/{plural}"
    jq = ('[.comps.items[] | {name: .metadata.name, marker: '
          f'(.metadata.annotations["{FRESH_ANNOTATION}"] // "")}}]')
    return f"""\
apiVersion: templates.krateo.io/v1
kind: RESTAction
metadata:
  name: {FRESH_RA_NAME}
  namespace: {ra_ns}
  labels:
    app: scaletest
spec:
  api:
  - name: comps
    path: "{path}"
    filter: '{jq}'
"""


def call_path(group: str, version: str, plural: str, name: str, ns: str) -> str:
    return (f"/call?apiVersion={group}%2F{version}&resource={plural}"
            f"&name={name}&namespace={ns}")


def kubectl_annotate(kubeconfig: str, context: str, resource: str, ns: str,
                     name: str, value: str, timeout: int = 30) -> bool:
    """`kubectl annotate --overwrite` with an EXPLICIT kubeconfig + context:
    the operator's ambient context is never consulted."""
    import subprocess
    argv = ["kubectl", f"--kubeconfig={kubeconfig}", f"--context={context}",
            "annotate", "--overwrite", resource, name, "-n", ns,
            f"{FRESH_ANNOTATION}={value}"]
    try:
        return subprocess.run(argv, capture_output=True,
                              timeout=timeout).returncode == 0
    except Exception:  # noqa: BLE001
        return False


# ─── CLI ────────────────────────────────────────────────────────────────────


def _jsonl_sink(path: str):
    lock = threading.Lock()
    fh = open(path, "a", buffering=1)

    def _w(rec):
        with lock:
            fh.write(json.dumps(rec, default=str) + "\n")
    return _w


def _read_jsonl(path: str) -> list[dict]:
    """Records from a JSONL file. `kubectl logs` interleaves the driver's
    stderr diagnostics with its JSONL; non-JSON lines are skipped and counted
    (a `{"kind": "skipped"}` record) so nothing disappears silently."""
    out = []
    skipped = 0
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            if not line.startswith("{"):
                skipped += 1
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                skipped += 1
    if skipped:
        out.append({"kind": "skipped", "path": path, "lines": skipped})
    return out


def _parse_pages(specs: list[str]) -> list[tuple[str, str, float]]:
    """`name=weight=/call?...` → (name, path, weight)."""
    out = []
    for s in specs:
        name, weight, path = s.split("=", 2)
        if not path.startswith("/call"):
            raise ValueError(f"page root must be a /call path: {s}")
        out.append((name, path, float(weight)))
    return out


def _cmd_drive(a) -> int:
    client = HttpClient(a.snowplow_url)
    users = [f"{a.user_prefix}{i:04d}" for i in range(a.user_start, a.user_end + 1)]
    tokens = {}
    for u in users:
        for attempt in range(3):
            try:
                tokens[u] = login(a.authn_url, u, a.password)
                break
            except Exception as e:  # noqa: BLE001
                if attempt == 2:
                    print(f"login failed for {u}: {e}", file=sys.stderr)
                time.sleep(1 + attempt)
    if len(tokens) < len(users):
        print(f"only {len(tokens)}/{len(users)} users logged in; refusing a "
              f"partial population", file=sys.stderr)
        return 2
    pages = _parse_pages(a.page)
    sink = _jsonl_sink(a.out)
    # Preflight: every root must answer 200 for the first user, else the page
    # names are stale for this portal version and the whole run would measure
    # error pages.
    probe_user = users[0]
    for name, root, _ in pages:
        r = client.get(root, tokens[probe_user])
        if r.status != 200:
            print(f"page root {name} → HTTP {r.status} {r.error}; fix --page",
                  file=sys.stderr)
            return 2
    stop = threading.Event()

    def _sampler():
        while not stop.is_set():
            v = read_vars(client, tokens[probe_user])
            sink({"kind": "vars", "t": time.time(),
                  **(summarize_vars(v) if v else {"unreadable": True})})
            stop.wait(a.vars_interval)

    th = None
    if a.vars_interval > 0:
        th = threading.Thread(target=_sampler, daemon=True)
        th.start()
    Driver(client, tokens, pages, think_s=a.think_s, duration_s=a.duration_s,
           nav_workers=a.nav_workers, call_workers=a.call_workers,
           sink=sink).run()
    stop.set()
    if th:
        th.join(timeout=a.vars_interval + 30)
    return 0


_SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"


def incluster_annotate(group: str, version: str, plural: str, ns: str,
                       name: str, value: str, *, sa_dir: str = _SA_DIR,
                       api: str = "https://kubernetes.default.svc",
                       timeout: float = 30) -> bool:
    """Merge-patch the freshness annotation through the Kubernetes API using
    the pod's ServiceAccount (the fresh606 Job). No kubectl, no kubeconfig."""
    import ssl
    try:
        with open(os.path.join(sa_dir, "token")) as fh:
            tok = fh.read().strip()
        ctx = ssl.create_default_context(cafile=os.path.join(sa_dir, "ca.crt"))
        body = json.dumps({"metadata": {"annotations": {FRESH_ANNOTATION: value}}}).encode()
        req = urllib.request.Request(
            f"{api}/apis/{group}/{version}/namespaces/{ns}/{plural}/{name}",
            data=body, method="PATCH",
            headers={"Authorization": "Bearer " + tok,
                     "Content-Type": "application/merge-patch+json"})
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return 200 <= r.status < 300
    except Exception:  # noqa: BLE001 — a failed mutate is recorded, not raised
        return False


def _cmd_fresh(a) -> int:
    """Freshness probe at one or more churn rates (labels list_dep@<rate>).

    --mutator kubectl    operator machine; explicit --kubeconfig/--context.
    --mutator incluster  inside the fresh606 Job; Kubernetes API via the
                         pod's ServiceAccount.
    """
    if a.mutator == "kubectl":
        if not a.kubeconfig or not a.context:
            print("--mutator kubectl needs --kubeconfig and --context", file=sys.stderr)
            return 2
        if "krateo-057" in a.context:
            print("refusing: krateo-057 is never a bench target (#606)", file=sys.stderr)
            return 3

        def patch(ns, name, value):
            return kubectl_annotate(a.kubeconfig, a.context, f"{a.plural}.{a.group}",
                                    ns, name, value)
    else:
        def patch(ns, name, value):
            return incluster_annotate(a.group, a.version, a.plural, ns, name, value)

    client = HttpClient(a.snowplow_url)
    token = None
    for attempt in range(5):
        try:
            token = login(a.authn_url, a.user, a.password)
            break
        except Exception as e:  # noqa: BLE001
            print(f"fresh: login attempt {attempt + 1} failed: {e}", file=sys.stderr)
            time.sleep(2 * (attempt + 1))
    if token is None:
        return 2
    sink = _jsonl_sink(a.out)
    # The probe object is excluded from churn: churn writes the same
    # annotation key and would overwrite the probe's marker.
    targets = [(f"bench-ns-{n:02d}", f"{a.name_prefix}{n:02d}-{k:02d}")
               for n in range(1, a.churn_namespaces + 1)
               for k in range(1, a.churn_per_ns + 1)
               if (f"bench-ns-{n:02d}", f"{a.name_prefix}{n:02d}-{k:02d}")
               != (a.namespace, a.probe_name)]
    path = call_path("templates.krateo.io", "v1", "restactions",
                     FRESH_RA_NAME, a.ra_namespace)
    rates = a.rates if a.rates else [a.churn_rate]
    for rate in rates:
        rate = float(rate)
        label = f"list_dep@{rate:g}"
        churn = None
        if rate > 0:
            churn = ChurnDriver(targets, rate,
                                lambda t, m: patch(t[0], t[1], m)).start()
        freshness_probe(
            client, token, path, lambda m: patch(a.namespace, a.probe_name, m),
            samples=a.samples, poll_ms=a.poll_ms, gap_s=a.gap_s,
            label=label, sink=sink)
        if churn:
            sink({"kind": "churn", "label": label, **churn.stop()})
    return 0


def _cmd_evaluate(a) -> int:
    recs = []
    for p in a.inputs:
        recs.extend(_read_jsonl(p))
    navs = [r for r in recs if r.get("kind") == "nav"]
    fresh = [r for r in recs if r.get("kind") == "fresh"]
    vs = sorted((r for r in recs if r.get("kind") == "vars"
                 and not r.get("unreadable")), key=lambda r: r.get("t", 0))
    result = evaluate(navs, fresh, vs)
    print(json.dumps(result, indent=2, default=str))
    return {"PASS": 0, "FAIL": 1, "INVALID": 3}[result["verdict"]]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="load606")
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("drive")
    d.add_argument("--snowplow-url", required=True)
    d.add_argument("--authn-url", required=True)
    d.add_argument("--password", required=True)
    d.add_argument("--user-prefix", default="scaleuser-")
    d.add_argument("--user-start", type=int, default=1)
    d.add_argument("--user-end", type=int, default=1000)
    d.add_argument("--page", action="append", required=True,
                   help="name=weight=/call?... (repeatable)")
    d.add_argument("--think-s", type=float, default=60.0)
    d.add_argument("--duration-s", type=float, default=1800.0)
    d.add_argument("--nav-workers", type=int, default=128)
    d.add_argument("--call-workers", type=int, default=512)
    d.add_argument("--vars-interval", type=float, default=15.0)
    d.add_argument("--out", required=True)
    d.set_defaults(func=_cmd_drive)
    f = sub.add_parser("fresh")
    f.add_argument("--snowplow-url", required=True)
    f.add_argument("--authn-url", required=True)
    f.add_argument("--user", required=True)
    f.add_argument("--password", required=True)
    f.add_argument("--kubeconfig", default=None)
    f.add_argument("--context", default=None)
    f.add_argument("--mutator", choices=["kubectl", "incluster"], default="kubectl")
    f.add_argument("--rates", nargs="*", default=None,
                   help="churn rates to step through (overrides --churn-rate)")
    f.add_argument("--group", default="composition.krateo.io")
    f.add_argument("--version", required=True)
    f.add_argument("--plural", required=True)
    f.add_argument("--namespace", default="bench-ns-01")
    f.add_argument("--probe-name", required=True)
    f.add_argument("--ra-namespace", default="bench-ns-01")
    f.add_argument("--samples", type=int, default=60)
    f.add_argument("--poll-ms", type=int, default=100)
    f.add_argument("--gap-s", type=float, default=5.0)
    f.add_argument("--churn-rate", type=float, default=0.0)
    f.add_argument("--churn-namespaces", type=int, default=50)
    f.add_argument("--churn-per-ns", type=int, default=20)
    f.add_argument("--name-prefix", default="bench-app-")
    f.add_argument("--out", required=True)
    f.set_defaults(func=_cmd_fresh)
    e = sub.add_parser("evaluate")
    e.add_argument("inputs", nargs="+")
    e.set_defaults(func=_cmd_evaluate)
    a = p.parse_args(argv)
    return int(a.func(a))


if __name__ == "__main__":
    sys.exit(main())
