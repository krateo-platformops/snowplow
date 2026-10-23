"""The paired customer-latency gate (#606, #865; Diego 2026-10-23): two runs
of the same bench on the same schedule, in parallel on two temporary
clusters — A (the baseline image) and B (the candidate) — and k6's customer
calls (every request k6 sends is a portal /call of the SPA's widget tree,
k6/navigate.js fetcher) compared per phase, B against A.

Each run's verdict.json carries report["customer_latency"] (report(): per
phase n, p95/p99 of http_req_duration, req/s and the half-sample spread of
each; the schedule; refresher completions/s, snowplow CPU and route clicks
against the open schedule per phase). `python3 -m bench.custlat pair` reads
the two verdicts and writes the paired verdict (pair()).

Phases are load606.run_phases' (by each call's completion time, the k6
point's time): storm, burst, churn:<label> per fresh606 step, and steady
(k6 still running, no churn). With run606's K6_SPAN=churn the customers stay
on through every churn step; pair() makes a pair whose k6 stopped before the
last step ended INVALID.

Per cell (phase x p95 / p99 / req/s), B / A against a tolerance:
  within   A's own sampling spread: paired half-samples of BLOCK_S blocks —
           adjacent blocks (2k, 2k+1) go one to each half, so both halves
           keep the phase's time profile (a storm is a ramp, not a
           stationary series); sd(log full) = sd(log(A1 / A2)) / 2. A's sd
           stands for both runs of the ratio: exp(z * sqrt(2) * sd). B's own
           sd is never used (a noisier candidate would loosen its own bar).
  replica  the observed ratio between two earlier runs of (nearly) the same
           code, for the phases they share with this schedule (a committed
           file, `custlat replica`): the run-level spread sampling cannot see.
  tolerance = max(within, replica); z = the one-sided normal quantile at
  FAMILY_ALPHA / cells, so the whole gate false-FAILs unchanged code at most
  FAMILY_ALPHA of the time under the within-run model.
Latency PASSes when B / A <= tolerance; req/s (k6 is closed-loop: a click
THINK_S after the page completed, so a slower snowplow shows as fewer req/s
on the same schedule) when B / A >= 1 / tolerance.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import itertools
import json
import math
import os
import random
import re
import statistics
import sys
from statistics import NormalDist

from bench import k6eval, load606

METRICS = ("p95_ms", "p99_ms", "req_s")
LOWER_IS_WORSE = ("req_s",)
BLOCK_S = 10.0
HALF_SAMPLES = 400
HALF_SEED = 606
FAMILY_ALPHA = 0.05
STEADY = "steady"
REFRESHER_KEY = "snowplow_refresher_completed_total"
# the schedule the two runs of a pair must share (k6-summary config)
SCHEDULE_KEYS = ("users", "navUsers", "burstUsers", "thinkS", "durationS", "burstAtS",
                 "navRate", "routes")
_TOP_CPU = re.compile(r"(\S+)\s+(\d+)m\s+\d+\w*")


def _pct(xs: list[float], p: float) -> float | None:
    """Linear-interpolated percentile (k6's own p(95) method)."""
    if not xs:
        return None
    v = sorted(xs)
    k = (len(v) - 1) * p / 100.0
    lo, hi = int(k), min(int(k) + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (k - lo)


def _names(wins) -> list[str]:
    return [n for n, _, _ in wins] + [STEADY]


def _seconds(a: float, b: float, wins: list[tuple[str, float, float]]) -> dict[str, float]:
    """[a, b) split by phase: each window takes what the earlier ones left;
    "steady" is what none took."""
    left = [(a, b)]
    out = dict.fromkeys(_names(wins), 0.0)
    for name, w0, w1 in wins:
        nxt = []
        for x, y in left:
            lo, hi = max(x, w0), min(y, w1)
            if lo < hi:
                out[name] += hi - lo
                nxt += [s for s in ((x, lo), (hi, y)) if s[0] < s[1]]
            else:
                nxt.append((x, y))
        left = nxt
    out[STEADY] += sum(y - x for x, y in left)
    return out


def _phase(t: float, wins: list[tuple[str, float, float]]) -> str:
    return next((n for n, w0, w1 in wins if w0 <= t < w1), STEADY)


def calls(o: dict | None) -> list[tuple[float, float]]:
    """k6's customer calls as (completion epoch, ms), those with a time."""
    if not o:
        return []
    ts = (o.get("times") or {}).get("http_req_duration") or []
    return [(ts[i], v) for i, (v, _) in enumerate(o["points"].get("http_req_duration", []))
            if i < len(ts) and ts[i] is not None and v is not None]


def blocks(cs: list[tuple[float, float]], wins) -> dict[str, list[tuple[list[float], float]]]:
    """Per phase, in time order: one (call ms list, seconds in the phase) per
    BLOCK_S block of k6's span (from its first call to its last) that the
    phase holds any of."""
    out: dict[str, list] = {p: [] for p in _names(wins)}
    if not cs:
        return out
    t0 = min(t for t, _ in cs)
    t1 = max(t for t, _ in cs)
    nb = int((t1 - t0) // BLOCK_S) + 1
    vals: dict[tuple[str, int], list[float]] = {}
    for t, v in cs:
        vals.setdefault((_phase(t, wins), int((t - t0) // BLOCK_S)), []).append(v)
    for k in range(nb):
        a = t0 + k * BLOCK_S
        secs = _seconds(a, min(a + BLOCK_S, t1), wins)
        for p, bl in out.items():
            if secs[p] > 0 or (p, k) in vals:
                bl.append((vals.get((p, k), []), secs[p]))
    return out


def _stat(bl: list[tuple[list[float], float]], metric: str) -> float | None:
    if metric == "req_s":
        secs = sum(s for _, s in bl)
        return sum(len(v) for v, _ in bl) / secs if secs > 0 else None
    return _pct([x for v, _ in bl for x in v], {"p95_ms": 95, "p99_ms": 99}[metric])


def half_sd(bl: list[tuple[list[float], float]], metric: str,
            n: int = HALF_SAMPLES, seed: int = HALF_SEED) -> float | None:
    """sd of log(metric) from paired half-samples (see the module doc)."""
    rnd = random.Random(seed)
    pairs = [(bl[i], bl[i + 1]) for i in range(0, len(bl) - 1, 2)]
    if len(pairs) < 2:
        return None
    logs = []
    for _ in range(n):
        a, b = [], []
        for x, y in pairs:
            if rnd.random() < 0.5:
                x, y = y, x
            a.append(x)
            b.append(y)
        sa, sb = _stat(a, metric), _stat(b, metric)
        if sa and sb:
            logs.append(math.log(sa / sb))
    return statistics.pstdev(logs) / 2 if len(logs) >= n // 2 else None


def phase_stats(cs: list[tuple[float, float]], wins) -> dict:
    """Per phase with any k6 second: n, seconds, p95/p99 ms, req/s and their
    half-sample sd."""
    out = {}
    for p, b in blocks(cs, wins).items():
        if not b:
            continue
        out[p] = {"n": sum(len(v) for v, _ in b), "seconds": round(sum(s for _, s in b), 1),
                  **{m: (round(x, 2) if (x := _stat(b, m)) is not None else None) for m in METRICS},
                  "half_sd": {m: (round(s, 4) if (s := half_sd(b, m)) is not None else None)
                              for m in METRICS}}
    return out


def schedule(o: dict | None, browser: dict | None, mem: list[dict],
             churns: list[dict] | None) -> dict:
    """What the two runs of a pair must share: k6's config, the CPU-profile
    plan (the storm and burst windows), the browser sessions and the churn
    steps (label, target rate) in order."""
    cfg = ((o or {}).get("summary") or {}).get("config") or {}
    plan = next((r.get("cpu_plan") for r in mem if r.get("kind") == "mem_start" and r.get("cpu_plan")),
                None)
    return {"k6": {k: cfg.get(k) for k in SCHEDULE_KEYS},
            "cpu_plan": plan,
            "browser_sessions": len(((browser or {}).get("summary") or {}).get("sessions") or []),
            "churn": [[c.get("label"), c.get("target_rate")] for c in churns or []
                      if c.get("kind") == "churn"]}


def refresher_per_phase(mem: list[dict], wins, span) -> dict:
    """Refresher completions/s per phase, while k6 ran (`with_customers`)
    and over the whole run (pair deltas by the later sample's time; a
    restart's pair counts the later value as-is)."""
    rows = sorted((m for m in mem if m.get("kind") == "mem"
                   and load606._num((m.get("extra") or {}).get(REFRESHER_KEY)) is not None),
                  key=lambda m: m["t"])
    if not rows:
        return {"status": load606.REPORT_ABSENT,
                "note": f"no mem sample carried {REFRESHER_KEY}: missing, not zero"}
    acc: dict[str, dict[str, list[float]]] = {}
    restarts = 0
    for a, b in itertools.pairwise(rows):
        ca = {"c": load606._num(a["extra"][REFRESHER_KEY])}
        cb = {"c": load606._num(b["extra"][REFRESHER_KEY])}
        d, r = load606._deltas(a, b, ca, cb)
        restarts += r
        p = acc.setdefault(_phase(b["t"], wins), {"whole_run": [0.0, 0.0], "with_customers": [0.0, 0.0]})
        for k in ("whole_run",) + (("with_customers",) if span and span[0] <= b["t"] <= span[1] else ()):
            p[k][0] += d["c"]
            p[k][1] += b["t"] - a["t"]

    def rate(x):
        return {"completed": int(x[0]), "seconds": round(x[1], 1),
                "per_s": round(x[0] / x[1], 3) if x[1] else None}
    return {"status": "present", "restarts": restarts,
            "by_phase": {p: {k: rate(v) for k, v in x.items()} for p, x in acc.items()}}


def cpu_per_phase(tops: list[dict], wins, span) -> dict:
    """snowplow's CPU (kubectl top, millicores summed over its pods per
    sample) per phase, while k6 ran and over the whole run."""
    acc: dict[str, dict[str, list[int]]] = {}
    for t in tops:
        ms = [int(m.group(2)) for m in _TOP_CPU.finditer(t.get("raw") or "")]
        if not ms or t.get("t") is None:
            continue
        p = acc.setdefault(_phase(t["t"], wins), {"whole_run": [], "with_customers": []})
        p["whole_run"].append(sum(ms))
        if span and span[0] <= t["t"] <= span[1]:
            p["with_customers"].append(sum(ms))

    def st(v):
        return ({"samples": len(v), "median_m": statistics.median(v), "p90_m": round(_pct(v, 90)),
                 "max_m": max(v)} if v else {"samples": 0})
    return {"by_phase": {p: {k: st(v) for k, v in x.items()} for p, x in acc.items()}}


def clicks_vs_schedule(o: dict | None, wins) -> dict | None:
    """Per phase: route clicks k6 made vs the open schedule's (each user's
    arrival + v * THINK_S, v >= 1, before the run's end; anchored on the first
    arrival). Report only: the closed loop defers a click by its page time."""
    cfg = ((o or {}).get("summary") or {}).get("config") or {}
    ts = (o or {}).get("times") or {}
    arr = [t for t in ts.get("arrivals") or [] if t is not None]
    if not arr or not cfg.get("thinkS") or not cfg.get("durationS"):
        return None
    end = min(arr) + cfg["durationS"]
    sched: dict[str, int] = {}
    for a in arr:
        v = 1
        while a + v * cfg["thinkS"] < end:
            p = _phase(a + v * cfg["thinkS"], wins)
            sched[p] = sched.get(p, 0) + 1
            v += 1
    done: dict[str, int] = {}
    for t in ts.get("clicks") or []:
        if t is not None:
            done[_phase(t, wins)] = done.get(_phase(t, wins), 0) + 1
    return {p: {"clicks": done.get(p, 0), "open_schedule": sched.get(p, 0),
                "achieved_share": round(done.get(p, 0) / sched[p], 4) if sched.get(p) else None}
            for p in _names(wins) if p in sched or p in done}


def report(k6o: dict | None, browser: dict | None, mem: list[dict], fresh: list[dict] | None,
           churns: list[dict] | None, tops: list[dict]) -> dict:
    """report["customer_latency"] of one run's verdict.json."""
    wins = load606.run_phases(mem, list(fresh or []) + list(churns or []))
    cs = calls(k6o)
    span = [min(t for t, _ in cs), max(t for t, _ in cs)] if cs else None
    return {"block_s": BLOCK_S, "calls": len(cs), "k6_span": span,
            "windows": [[n, w0, w1] for n, w0, w1 in wins],
            "phases": phase_stats(cs, wins),
            "schedule": schedule(k6o, browser, mem, churns),
            "clicks_vs_open_schedule": clicks_vs_schedule(k6o, wins),
            "refresher_completions": refresher_per_phase(mem, wins, span),
            "snowplow_cpu": cpu_per_phase(tops, wins, span)}


def _validity(name, ok, value, bar, why):
    return {"check": f"customer_latency.pair.{name}", "value": value, "bar": bar,
            "result": "PASS" if ok else "FAIL", **({} if ok else {"why": why})}


# a run's own verdict that can be compared (INVALID cannot; a missing or
# unknown one is not a run's verdict at all)
COMPARABLE_VERDICTS = ("PASS", "FAIL", "INCONCLUSIVE")


def _measure(x) -> bool:
    """A latency / rate / spread value a ratio can be taken of: a finite
    number, positive (sd: not negative)."""
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and x > 0


def pair(va: dict, vb: dict, *, replica: dict | None = None,
         expect_a: str | None = None, expect_b: str | None = None,
         completed: tuple[bool | None, bool | None] = (None, None)) -> dict:
    """The paired verdict: B's customer latency per phase against A's (module
    doc). INVALID when the pair cannot be compared; FAIL when a cell is past
    its tolerance. `completed`: whether each run's evaluate step ran (the
    launcher's reading of its run.log); None = not known."""
    va, vb = va or {}, vb or {}
    ra = (va.get("report") or {}).get("customer_latency")
    rb = (vb.get("report") or {}).get("customer_latency")
    dig = [((v.get("report") or {}).get("image_digest") or {}).get("expected") for v in (va, vb)]
    validity = [
        _validity("runs_valid", va.get("verdict") in COMPARABLE_VERDICTS and vb.get("verdict") in
                  COMPARABLE_VERDICTS, {"a": va.get("verdict"), "b": vb.get("verdict")},
                  f"both runs' own verdicts in {COMPARABLE_VERDICTS}",
                  "a run whose own measurement is INVALID (or that has no verdict) cannot be compared"),
        _validity("runs_completed", False not in completed,
                  {"a": completed[0], "b": completed[1]}, "both runs' evaluate step ran",
                  "a run failed as tooling before its evaluate step: its verdict is partial"),
        _validity("k6_reports", ra is not None and rb is not None,
                  {"a": ra is not None, "b": rb is not None}, "both verdicts carry customer_latency",
                  "a run without k6 has no customer calls"),
        _validity("images", None not in dig and dig[0] != dig[1]
                  and (expect_a is None or dig[0] == expect_a) and (expect_b is None or dig[1] == expect_b),
                  {"a": dig[0], "b": dig[1]}, "two different images, each the one its run was launched with",
                  "the pair is not the two images it was launched to compare"),
    ]
    out = {"kind": "customer_latency_pair", "checks": [], "cells": [], "validity": validity,
           "method": {"block_s": BLOCK_S, "half_samples": HALF_SAMPLES, "half_seed": HALF_SEED,
                      "family_alpha": FAMILY_ALPHA,
                      "tolerance": "max(exp(z * sqrt(2) * A's half-sample sd), replica ratio); "
                                   "latency: B / A <= tolerance; req_s: B / A >= 1 / tolerance",
                      "replica": (replica or {}).get("provenance")}}
    if ra is not None and rb is not None:
        diff = sorted(k for k in set(ra["schedule"]) | set(rb["schedule"])
                      if ra["schedule"].get(k) != rb["schedule"].get(k))
        validity.append(_validity("schedules_match", not diff,
                                  {k: {"a": ra["schedule"].get(k), "b": rb["schedule"].get(k)}
                                   for k in diff} or "same",
                                  "the k6 config, CPU-profile plan, browser sessions and churn steps",
                                  f"{', '.join(diff)} differ: the comparison would measure the schedule"))
        for name, r in (("a", ra), ("b", rb)):
            last = max((w1 for n, _, w1 in r["windows"] if n.startswith("churn:")), default=None)
            span = r.get("k6_span")
            validity.append(_validity(
                f"customers_through_churn[{name}]", last is not None and span is not None and span[1] >= last,
                {"k6_last_call": span[1] if span else None, "last_churn_end": last},
                "k6's last call after the last churn step ended",
                "the customers stopped before the churn did: a churn step has no customer latency"))
        cells = [(p, m) for p, s in ra["phases"].items() if s["n"] for m in METRICS]
        z = NormalDist().inv_cdf(1 - FAMILY_ALPHA / len(cells)) if cells else None
        out["method"]["cells"], out["method"]["z"] = len(cells), round(z, 4) if z else None
        for p in dict.fromkeys(p for p, _ in cells):
            sa, sb = ra["phases"][p], rb["phases"].get(p) or {}
            if not sb.get("n"):
                validity.append(_validity(f"measured[{p}]", False, 0, "> 0 k6 calls in B",
                                          f"B has no k6 call in {p}: the cell cannot be compared"))
                continue
            for m in METRICS:
                sd, a, b = sa["half_sd"][m], sa[m], sb.get(m)
                rep = (((replica or {}).get("phases") or {}).get(p) or {}).get(m)
                # finite, positive measurements only: a 0 A, a negative p95,
                # an infinite spread or replica would make the ratio or the
                # bar meaningless (a +Inf spread passed anything)
                if not (_measure(a) and _measure(b) and (_measure(sd) or sd == 0)
                        and (rep is None or (_measure(rep) and rep >= 1))):
                    validity.append(_validity(f"spread[{p}].{m}", False,
                                              {"a": a, "b": b, "a_half_sd": sd, "replica": rep},
                                              "finite positive A, B, A's half-sample sd >= 0, replica >= 1",
                                              f"{p} {m}: not a measurement, no tolerance can be derived"))
                    continue
                within = math.exp(z * math.sqrt(2) * sd)
                tol = max(within, rep or 1.0)
                lower = m in LOWER_IS_WORSE
                bar = round(1 / tol, 4) if lower else round(tol, 4)
                ratio = round(b / a, 4)
                ok = ratio >= bar if lower else ratio <= bar
                out["cells"].append({
                    "phase": p, "metric": m, "a": a, "b": b, "ratio": ratio,
                    "bar": f"{'>=' if lower else '<='} {bar}", "within": round(within, 4), "replica": rep,
                    "result": "PASS" if ok else "FAIL",
                    "line": f"{'PASS' if ok else 'FAIL'} {p} {m}: B/A = {ratio} "
                            f"({'>=' if lower else '<='} {bar}; a {a}, b {b}; within {round(within, 4)}, "
                            f"replica {rep})"})
                out["checks"].append({
                    "check": f"customer_latency.pair[{p}].{m}",
                    "value": {"a": a, "b": b, "ratio": ratio},
                    "bar": f"ratio {'>=' if lower else '<='} {bar} (within {round(within, 4)} from A's "
                           f"sd {sd}; replica {rep})",
                    "result": "PASS" if ok else "FAIL",
                    **({} if ok else {"why": f"{p} {m}: B is {ratio}x A, beyond the spread ({bar})"})})
    if not out["checks"] and all(c["result"] == "PASS" for c in validity):
        validity.append(_validity("cells", False, 0, ">= 1 cell", "A has no phase with k6 calls"))
    out["verdict"] = ("INVALID" if any(c["result"] == "FAIL" for c in validity) else
                      "FAIL" if any(c["result"] == "FAIL" for c in out["checks"]) else "PASS")
    out["invalid_reasons"] = [f"{c['check']}: {c['why']}" for c in validity if c["result"] == "FAIL"]
    # beside the verdict: both runs' per-phase reports, side by side
    out["phases"] = {p: {"a": (ra or {}).get("phases", {}).get(p), "b": (rb or {}).get("phases", {}).get(p)}
                     for p in dict.fromkeys(list((ra or {}).get("phases", {})) +
                                            list((rb or {}).get("phases", {})))}
    for k in ("refresher_completions", "snowplow_cpu", "clicks_vs_open_schedule"):
        out[k] = {"a": (ra or {}).get(k), "b": (rb or {}).get(k)}
    return out


def replica_ratios(r1: dict, r2: dict, provenance: dict) -> dict:
    """The replica file: per phase both reports measured, |log| ratio per
    metric (either direction: the spread's size)."""
    phases = {}
    for p, s1 in r1["phases"].items():
        s2 = r2["phases"].get(p)
        if not (s1.get("n") and s2 and s2.get("n")):
            continue
        phases[p] = {m: round(math.exp(abs(math.log(s1[m] / s2[m]))), 4)
                     for m in METRICS if s1.get(m) and s2.get(m)}
    return {"schema": "customer_latency_replica/1", "provenance": provenance, "phases": phases}


def load_replica(path: str) -> dict:
    with open(path) as fh:
        r = json.load(fh)
    if r.get("schema") != "customer_latency_replica/1":
        raise ValueError(f"{path}: not a customer_latency_replica/1 file")
    return r


def report_from_dir(d: str) -> dict:
    """A collected run directory's report (run606's $OUT layout)."""
    def jsonl(pattern):
        out = []
        for p in sorted(glob.glob(os.path.join(d, pattern))):
            out += load606._read_jsonl(p)
        return out
    mem = [r for r in jsonl("mem-*.jsonl") if r.get("kind") in
           ("mem_start", "mem", "heap_profile", "cpu_profile", "mem_end")]
    fr = jsonl("fresh-*.jsonl")
    tops = [r for r in jsonl("top.jsonl") if r.get("kind") == "top"]
    return report(k6eval.read_k6_out(os.path.join(d, "k6"), "k6"),
                  k6eval.read_k6_out(os.path.join(d, "browser"), "browser"), mem,
                  [r for r in fr if r.get("kind") == "fresh"],
                  [r for r in fr if r.get("kind") == "churn"], tops)


def _sha256(p: str) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _provenance(d: str) -> dict:
    v = {}
    try:
        with open(os.path.join(d, "verdict.json")) as fh:
            v = json.load(fh)
    except (OSError, ValueError):
        pass
    k6s = os.path.join(d, "k6", "k6.json.gz")
    return {"dir": os.path.basename(os.path.normpath(d)), "verdict": v.get("verdict"),
            "image_digest": ((v.get("report") or {}).get("image_digest") or {}).get("expected"),
            "k6_stream_sha256": _sha256(k6s) if os.path.exists(k6s) else None}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="custlat")
    sub = ap.add_subparsers(dest="cmd", required=True)
    pr = sub.add_parser("pair", help="the paired verdict of two runs' verdict.json (A baseline, B candidate)")
    pr.add_argument("--a", required=True, help="A's verdict.json (the baseline image)")
    pr.add_argument("--b", required=True, help="B's verdict.json (the candidate image)")
    pr.add_argument("--replica", default=None, help="a customer_latency_replica/1 file")
    pr.add_argument("--expect-a", default=None, help="A's EXPECTED_IMAGE_DIGEST")
    pr.add_argument("--expect-b", default=None, help="B's EXPECTED_IMAGE_DIGEST")
    for x in ("a", "b"):
        pr.add_argument(f"--completed-{x}", type=int, choices=(0, 1), default=None,
                        help=f"1 when {x.upper()}'s evaluate step ran (run606 logged 'run complete')")
    rp = sub.add_parser("replica", help="the replica ratios of two collected run dirs")
    rp.add_argument("dirs", nargs=2)
    a = ap.parse_args(argv)
    if a.cmd == "replica":
        out = replica_ratios(report_from_dir(a.dirs[0]), report_from_dir(a.dirs[1]),
                             [_provenance(d) for d in a.dirs])
        json.dump(out, sys.stdout, indent=1, sort_keys=True)
        print()
        return 0

    def load(p):
        try:
            with open(p) as fh:
                return json.load(fh)
        except (OSError, ValueError):
            return None          # pair() makes it INVALID, with the reason
    done = tuple(None if c is None else bool(c) for c in (a.completed_a, a.completed_b))
    try:
        out = pair(load(a.a), load(a.b), replica=load_replica(a.replica) if a.replica else None,
                   expect_a=a.expect_a, expect_b=a.expect_b, completed=done)
    except Exception as e:  # noqa: BLE001 — a crash must read INVALID, never exit 1 = FAIL
        out = {"kind": "customer_latency_pair", "verdict": "INVALID", "checks": [], "cells": [],
               "invalid_reasons": [f"the paired verdict could not be computed: {type(e).__name__}: {e}"]}
    json.dump(out, sys.stdout, indent=1, default=str)
    print()
    return {"PASS": 0, "FAIL": 1, "INVALID": 3}[out["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
