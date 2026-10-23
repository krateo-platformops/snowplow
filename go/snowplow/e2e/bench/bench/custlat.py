"""Run 15's customer-latency gate (#606, #865): k6's customer calls (every
request k6 sends is a portal /call of the SPA's widget tree, k6/navigate.js
fetcher) per phase, against a committed baseline run, with a PASS/FAIL bar
derived from run-to-run spread data.

Phases (by each call's completion time, the k6 point's time):
  storm   the login storm window, burst  the composite deep-link burst
          window (both load606.run_phases: the CPU-profile plan's windows)
  rest    every other second k6 ran (between the storm and the burst, and
          after the burst, whichever churn step fresh606 was in)

Per phase: n, p95/p99 of http_req_duration, req/s (calls per second of the
phase while k6 ran). k6 is closed-loop (a click THINK_S after the page
completed), so a slower snowplow shows as fewer req/s at the same schedule;
the gate requires the run's schedule to be the baseline's (else INVALID), so
achieved req/s against the baseline's IS achieved against the schedule.

The spread (derive_baseline; the numbers and their provenance live in the
baseline file, never here):
  within   each run's own sampling spread: paired half-samples of BLOCK_S
           blocks — adjacent blocks (2k, 2k+1) go one to each half, so both
           halves keep the phase's time profile (a storm is a ramp, not a
           stationary series); sd(log full) = sd(log(A / B)) / 2. The spread
           runs' largest sd stands for both the baseline's and the gated
           run's (the gated run's own sd is never used: a noisier run would
           loosen its own bar). Bound: exp(z * sqrt(2) * sd).
  replica  the observed ratio between two runs of (nearly) the same code.
  tolerance = max(within bound, replica ratio); z = the one-sided normal
  quantile at FAMILY_ALPHA / cells, so the whole gate false-FAILs unchanged
  code at most FAMILY_ALPHA of the time under the within-run model.
Latency PASSes when run / baseline <= tolerance; req/s when run / baseline
>= 1 / tolerance.
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

PHASES = ("storm", "burst", "rest")
METRICS = ("p95_ms", "p99_ms", "req_s")
LOWER_IS_WORSE = ("req_s",)
BLOCK_S = 10.0
HALF_SAMPLES = 400
HALF_SEED = 606
FAMILY_ALPHA = 0.05
REFRESHER_KEY = "snowplow_refresher_completed_total"
# the schedule a gated run must share with its baseline (k6-summary config)
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


def windows(mem: list[dict], fresh: list[dict] | None) -> list[tuple[str, float, float]]:
    """The storm and burst windows (precedence order, as run_phases)."""
    return [w for w in load606.run_phases(mem, fresh) if w[0] in ("storm", "burst")]


def _seconds(a: float, b: float, wins: list[tuple[str, float, float]]) -> dict[str, float]:
    """[a, b) split by phase: each window takes what the earlier ones left;
    "rest" is what none took."""
    left = [(a, b)]
    out = dict.fromkeys(PHASES, 0.0)
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
    out["rest"] += sum(y - x for x, y in left)
    return out


def _phase(t: float, wins: list[tuple[str, float, float]]) -> str:
    return next((n for n, w0, w1 in wins if w0 <= t < w1), "rest")


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
    if not cs:
        return {p: [] for p in PHASES}
    t0 = min(t for t, _ in cs)
    t1 = max(t for t, _ in cs)
    nb = int((t1 - t0) // BLOCK_S) + 1
    vals: dict[tuple[str, int], list[float]] = {}
    for t, v in cs:
        vals.setdefault((_phase(t, wins), int((t - t0) // BLOCK_S)), []).append(v)
    out: dict[str, list] = {p: [] for p in PHASES}
    for k in range(nb):
        a = t0 + k * BLOCK_S
        secs = _seconds(a, min(a + BLOCK_S, t1), wins)
        for p in PHASES:
            if secs[p] > 0 or (p, k) in vals:
                out[p].append((vals.get((p, k), []), secs[p]))
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


def phase_stats(o: dict | None, mem: list[dict], fresh: list[dict] | None) -> dict:
    """Per phase: n, seconds, p95/p99 ms, req/s and their half-sample sd;
    and the same (no sd) by load606.run_phases label, for context."""
    wins = windows(mem, fresh)
    cs = calls(o)
    bl = blocks(cs, wins)
    out = {}
    for p in PHASES:
        b = bl[p]
        out[p] = {"n": sum(len(v) for v, _ in b), "seconds": round(sum(s for _, s in b), 1),
                  **{m: (round(x, 2) if (x := _stat(b, m)) is not None else None) for m in METRICS},
                  "half_sd": {m: (round(s, 4) if (s := half_sd(b, m)) is not None else None)
                              for m in METRICS}}
    rp = load606.run_phases(mem, fresh)
    by: dict[str, list[float]] = {}
    for t, v in cs:
        by.setdefault(load606._phase_of(t, rp), []).append(v)
    return {"block_s": BLOCK_S, "calls": len(cs),
            "k6_span": [min(t for t, _ in cs), max(t for t, _ in cs)] if cs else None,
            "windows": [[n, w0, w1] for n, w0, w1 in wins],
            "phases": out,
            "by_run_phase": {k: {"n": len(v), "p95_ms": round(_pct(v, 95), 2),
                                 "p99_ms": round(_pct(v, 99), 2)} for k, v in by.items()}}


def schedule(o: dict | None, browser: dict | None, mem: list[dict],
             churns: list[dict] | None) -> dict:
    """What the gated run must share with its baseline: k6's config, the
    CPU-profile plan (the phase windows), the browser sessions and the churn
    steps (label, target rate) in order."""
    cfg = ((o or {}).get("summary") or {}).get("config") or {}
    plan = next((r.get("cpu_plan") for r in mem if r.get("kind") == "mem_start" and r.get("cpu_plan")),
                None)
    return {"k6": {k: cfg.get(k) for k in SCHEDULE_KEYS},
            "cpu_plan": plan,
            "browser_sessions": len(((browser or {}).get("summary") or {}).get("sessions") or []),
            "churn": [[c.get("label"), c.get("target_rate")] for c in churns or []
                      if c.get("kind") == "churn"]}


def refresher_per_phase(mem: list[dict], wins, span, fresh=None) -> dict:
    """Refresher completions/s per gate phase while k6 ran, and per
    run_phases label over the whole run (pair deltas by the later sample's
    time; a restart's pair counts the later value as-is)."""
    rows = sorted((m for m in mem if m.get("kind") == "mem"
                   and load606._num((m.get("extra") or {}).get(REFRESHER_KEY)) is not None),
                  key=lambda m: m["t"])
    if not rows:
        return {"status": load606.REPORT_ABSENT,
                "note": f"no mem sample carried {REFRESHER_KEY}: missing, not zero"}
    rp = load606.run_phases(mem, fresh)
    gate: dict[str, list[float]] = {p: [0.0, 0.0] for p in PHASES}
    whole: dict[str, list[float]] = {}
    restarts = 0
    for a, b in itertools.pairwise(rows):
        ca = {"c": load606._num(a["extra"][REFRESHER_KEY])}
        cb = {"c": load606._num(b["extra"][REFRESHER_KEY])}
        d, r = load606._deltas(a, b, ca, cb)
        restarts += r
        dt = b["t"] - a["t"]
        w = whole.setdefault(load606._phase_of(b["t"], rp), [0.0, 0.0])
        w[0] += d["c"]
        w[1] += dt
        if span and span[0] <= b["t"] <= span[1]:
            g = gate[_phase(b["t"], wins)]
            g[0] += d["c"]
            g[1] += dt

    def rate(x):
        return {"completed": int(x[0]), "seconds": round(x[1], 1),
                "per_s": round(x[0] / x[1], 3) if x[1] else None}
    return {"status": "present", "restarts": restarts,
            "by_phase": {p: rate(gate[p]) for p in PHASES},
            "by_run_phase": {k: rate(v) for k, v in whole.items()}}


def cpu_per_phase(tops: list[dict], wins, span, mem=None, fresh=None) -> dict:
    """snowplow's CPU (kubectl top, millicores summed over its pods per
    sample) per gate phase while k6 ran, and per run_phases label."""
    rp = load606.run_phases(mem or [], fresh)
    gate: dict[str, list[int]] = {p: [] for p in PHASES}
    whole: dict[str, list[int]] = {}
    for t in tops:
        ms = [int(m.group(2)) for m in _TOP_CPU.finditer(t.get("raw") or "")]
        if not ms or t.get("t") is None:
            continue
        whole.setdefault(load606._phase_of(t["t"], rp), []).append(sum(ms))
        if span and span[0] <= t["t"] <= span[1]:
            gate[_phase(t["t"], wins)].append(sum(ms))

    def st(v):
        return ({"samples": len(v), "median_m": statistics.median(v), "p90_m": round(_pct(v, 90)),
                 "max_m": max(v)} if v else {"samples": 0})
    return {"by_phase": {p: st(gate[p]) for p in PHASES},
            "by_run_phase": {k: st(v) for k, v in whole.items()}}


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
    sched = dict.fromkeys(PHASES, 0)
    for a in arr:
        v = 1
        while a + v * cfg["thinkS"] < end:
            sched[_phase(a + v * cfg["thinkS"], wins)] += 1
            v += 1
    done = dict.fromkeys(PHASES, 0)
    for t in ts.get("clicks") or []:
        if t is not None:
            done[_phase(t, wins)] += 1
    return {p: {"clicks": done[p], "open_schedule": sched[p],
                "achieved_share": round(done[p] / sched[p], 4) if sched[p] else None}
            for p in PHASES}


def report(k6o: dict | None, browser: dict | None, mem: list[dict], fresh: list[dict] | None,
           churns: list[dict] | None, tops: list[dict]) -> dict:
    src = list(fresh or []) + list(churns or [])
    st = phase_stats(k6o, mem, src)
    wins = windows(mem, src)
    span = st["k6_span"]
    return {"stats": st, "schedule": schedule(k6o, browser, mem, churns),
            "clicks_vs_open_schedule": clicks_vs_schedule(k6o, wins),
            "refresher_completions": refresher_per_phase(mem, wins, span, src),
            "snowplow_cpu": cpu_per_phase(tops, wins, span, mem, src)}


def gate(rep: dict, baseline: dict) -> tuple[list[dict], list[dict]]:
    """(checks, validity): one check per baseline cell; INVALID when the
    run's schedule is not the baseline's or a baseline phase has no calls."""
    name = baseline["baseline_run"]
    validity = []
    want, have = baseline["schedule"], rep["schedule"]
    diff = sorted(k for k in set(want) | set(have) if want.get(k) != have.get(k))
    validity.append({"check": f"customer_latency.schedule_matches_{name}",
                     "value": {k: {"run": have.get(k), name: want.get(k)} for k in diff} or "same",
                     "bar": f"the k6 config, CPU-profile plan, browser sessions and churn steps of {name}",
                     "result": "FAIL" if diff else "PASS",
                     **({"why": f"{', '.join(diff)} differ from {name}: the latency comparison "
                                f"would measure the schedule, not snowplow"} if diff else {})})
    checks = []
    for p, cells in baseline["phases"].items():
        got = rep["stats"]["phases"].get(p) or {}
        if not got.get("n"):
            validity.append({"check": f"customer_latency.vs_{name}[{p}].measured", "value": 0,
                             "bar": "> 0 k6 calls", "result": "FAIL",
                             "why": f"no k6 call completed in the {p} phase: the gate cannot be measured"})
            continue
        for m, c in cells.items():
            v, b, tol = got.get(m), c["baseline"], c["tolerance"]
            ratio = round(v / b, 4) if v is not None and b else None
            lower = m in LOWER_IS_WORSE
            bar = round(1 / tol, 4) if lower else tol
            ok = ratio is not None and (ratio >= bar if lower else ratio <= bar)
            checks.append({"check": f"customer_latency.vs_{name}[{p}].{m}",
                           "value": {"run": v, name: b, "ratio": ratio},
                           "bar": f"ratio {'>=' if lower else '<='} {bar} "
                                  f"(run-to-run spread, set by {c['set_by']})",
                           "result": "PASS" if ok else "FAIL",
                           **({} if ok else {"why": f"{p} {m} is {ratio}x {name}'s, beyond the "
                                                    f"spread ({bar})"})})
    return checks, validity


def derive_baseline(name: str, base: dict, spread: dict[str, dict],
                    replica: tuple[dict, dict], provenance: dict) -> dict:
    """The baseline file: base's per-phase stats and each cell's tolerance
    (module doc). `spread` = {run: report} whose half-sample sd stand for
    the within-run spread; `replica` = two reports of (nearly) the same code."""
    cells = [(p, m) for p in PHASES for m in METRICS
             if base["stats"]["phases"][p]["n"] and base["stats"]["phases"][p][m]]
    if not cells:
        raise ValueError(f"{name}: no phase with k6 calls, nothing to gate")
    z = NormalDist().inv_cdf(1 - FAMILY_ALPHA / len(cells))
    phases: dict[str, dict] = {}
    for p, m in cells:
        sds = {r: rep["stats"]["phases"][p]["half_sd"][m] for r, rep in spread.items()}
        known = [s for s in sds.values() if s is not None]
        within = math.exp(z * math.sqrt(2) * max(known)) if known else None
        a, b = (rep["stats"]["phases"][p][m] for rep in replica)
        rep_ratio = math.exp(abs(math.log(a / b))) if a and b else None
        tol = max(x for x in (within, rep_ratio, 1.0) if x is not None)
        phases.setdefault(p, {})[m] = {
            "baseline": base["stats"]["phases"][p][m],
            "tolerance": round(tol, 4),
            "set_by": ("replica" if rep_ratio is not None and rep_ratio >= (within or 0) else "within"),
            "within_bound": round(within, 4) if within else None,
            "within_sd": sds,
            "replica_ratio": round(rep_ratio, 4) if rep_ratio else None}
    return {"schema": "customer_latency_baseline/1", "baseline_run": name,
            "method": {"block_s": BLOCK_S, "half_samples": HALF_SAMPLES, "half_seed": HALF_SEED,
                       "family_alpha": FAMILY_ALPHA, "cells": len(cells), "z": round(z, 4),
                       "tolerance": "max(exp(z * sqrt(2) * max within_sd), replica_ratio); "
                                    "latency: run / baseline <= tolerance; req_s: run / baseline "
                                    ">= 1 / tolerance"},
            "schedule": base["schedule"], "provenance": provenance,
            "baseline_stats": base["stats"]["phases"], "phases": phases}


def load_baseline(path: str) -> dict:
    with open(path) as fh:
        b = json.load(fh)
    if b.get("schema") != "customer_latency_baseline/1":
        raise ValueError(f"{path}: not a customer_latency_baseline/1 file")
    return b


def env_mismatches(baseline: dict, *, users: int, burst_users: int, think_s: int, drive_s: int,
                   burst_at_s: int, browser_sessions: int, modes: list[str],
                   rates: list[str]) -> list[str]:
    """Before any cloud call: the run606 knobs whose values would make the
    run's schedule differ from the baseline's (gate() would call the run
    INVALID only after it was paid for). navUsers / navRate / routes are
    derived at run time and stay with gate()."""
    s = baseline["schedule"]
    want = {"USERS": (users, s["k6"].get("users")),
            "COMPOSITE_BURST_USERS": (burst_users, s["k6"].get("burstUsers")),
            "THINK_S": (think_s, s["k6"].get("thinkS")),
            "DRIVE_S": (drive_s, s["k6"].get("durationS")),
            "COMPOSITE_BURST_AT_S": (burst_at_s, s["k6"].get("burstAtS")),
            "BROWSER_SESSIONS": (browser_sessions, s.get("browser_sessions")),
            # fresh_phases: modes outer, rates inner
            "FRESH_MODES x CHURN_RATES": (
                [f"list_dep{'' if m == 'annotation' else '_' + m}@{r}" for m in modes for r in rates],
                [label for label, _ in s.get("churn") or []])}
    return [f"{k}={got} but {baseline['baseline_run']} ran {base}" for k, (got, base) in want.items()
            if got != base]


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
    dv = sub.add_parser("derive", help="write a baseline file from collected run dirs")
    dv.add_argument("--name", required=True, help="the baseline's name in check names (e.g. run14)")
    dv.add_argument("--baseline", required=True, help="the baseline run's dir")
    dv.add_argument("--spread", action="append", required=True, metavar="NAME=DIR",
                    help="a run whose within-run spread is used (repeat)")
    dv.add_argument("--replica", nargs=2, required=True, metavar=("NAME=DIR", "NAME=DIR"),
                    help="two runs of (nearly) the same code")
    ce = sub.add_parser("check-env", help="exit 2 unless run606's knobs give the baseline's schedule")
    ce.add_argument("baseline")
    for f in ("users", "burst-users", "think-s", "drive-s", "burst-at-s", "browser-sessions"):
        ce.add_argument(f"--{f}", type=int, required=True)
    ce.add_argument("--modes", required=True, help="FRESH_MODES (space-separated)")
    ce.add_argument("--rates", required=True, help="CHURN_RATES (space-separated)")
    a = ap.parse_args(argv)
    if a.cmd == "check-env":
        bad = env_mismatches(load_baseline(a.baseline), users=a.users, burst_users=a.burst_users,
                             think_s=a.think_s, drive_s=a.drive_s, burst_at_s=a.burst_at_s,
                             browser_sessions=a.browser_sessions, modes=a.modes.split(),
                             rates=a.rates.split())
        for b in bad:
            print(b, file=sys.stderr)
        return 2 if bad else 0
    named = {}
    for s in [f"{a.name}={a.baseline}"] + a.spread + a.replica:
        n, _, d = s.partition("=")
        named.setdefault(n, (d, report_from_dir(d)))
    spread = {n.partition("=")[0]: named[n.partition("=")[0]][1] for n in a.spread}
    rn = [s.partition("=")[0] for s in a.replica]
    out = derive_baseline(a.name, named[a.name][1], spread, (named[rn[0]][1], named[rn[1]][1]),
                          {n: _provenance(d) | {"role": [r for r, ok in (
                              ("baseline", n == a.name), ("spread", n in spread),
                              ("replica", n in rn)) if ok]} for n, (d, _) in named.items()})
    json.dump(out, sys.stdout, indent=1, sort_keys=True)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
