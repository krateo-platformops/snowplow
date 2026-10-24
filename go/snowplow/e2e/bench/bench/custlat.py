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

Judged cells (Diego 2026-10-23, option A): 3 coarse phases x p95 / p99 /
req/s = 9 cells — storm, churn (EVERY churn step pooled, while the customers
are on) and steady. burst is shown, never judged (one ~15 s stall sets its
tail: run 14's 99 % of >p95 burst calls fell in 150-165 s); each churn step
on its own is shown, never judged. Shown cells print their ratio against
the bar the same method gives them.

Per cell, B / A against a tolerance:
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

Restarts and Ready time (run 15: both arms OOMKilled twice; team lead
2026-10-24):
  - any snowplow restart (or pod replacement) in B, the candidate, FAILs the
    pair outright, whatever else the pair is (never INVALID, never hidden);
    so does any stretch B was not Ready WITHOUT restarting (coordinator
    2026-10-24: its users saw errors; cutting its windows would hide that) —
    reported with its seconds and its signal (readyz codes, prewarm);
  - a restart of A, the baseline, is recorded and reported; it does not void
    the pair. Only a restart the readiness samples cannot place in time does
    (its recovery could not be excluded);
  - every cell is computed only over the time BOTH arms' snowplow was Ready.
    Each run's readiness (readiness(): mem606's 15 s /readyz samples, the
    prewarm-complete signal, counter resets, the pod records) gives the
    stretches it was not: from the last Ready sample before a restart to the
    first Ready sample with its cold start complete. Each stretch is removed
    from its own run AND, mapped by phase-relative time (the arms start
    minutes apart and their churn steps last differently), from the other;
  - a cell whose jointly-Ready sample is too short to decide is
    INCONCLUSIVE: A's half-sample sd on the kept blocks, scaled by the
    1/sqrt(n) law to the phase's full length, gives the bar a full-length
    sample would hold. PASS needs B / A within that bar, FAIL needs B / A
    past the kept sample's own (wider) bar, and in between the shortened
    overlap is what decides: INCONCLUSIVE. With nothing excluded the two
    bars are the same and the cell is decided as before;
  - every cell says how many seconds each arm lost and why;
  - A's minutes before a crash are Ready, so they count. Each cell also
    shows (never judges) an A-healthy sensitivity: the same cell with every
    phase A restarted in dropped whole from both runs; when B passes a judged
    cell A restarted inside, a NOTE says so;
  - k6's error rate (> 1 % voids) is counted only inside the jointly-Ready
    time, and only B's voids; A's whole-run rate is reported beside it.
    Every other reason a run's own verdict is INVALID still voids the pair.
"""

from __future__ import annotations

import argparse
import bisect
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
CHURN = "churn"
JUDGED = ("storm", CHURN, "steady")       # option A: the gated phases
REQUIRED = ("storm", CHURN)               # a pair without them judges nothing that matters
SHOWN = ("burst",)                        # shown, never judged (plus each churn step)
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
READY_STATUS = 200
RESTART_CHECK = "snowplow.restarts_during_measure"
ERROR_RATE_CHECK = "k6.error_rate"
INCONCLUSIVE = "INCONCLUSIVE"


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


def _union(ivs) -> list[tuple[float, float]]:
    """Intervals merged, sorted, non-empty."""
    out: list[list[float]] = []
    for a, b in sorted((a, b) for a, b in ivs if b > a):
        if out and a <= out[-1][1]:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return [(a, b) for a, b in out]


def _minus(a: float, b: float, ex: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """[a, b) without the (merged, sorted) intervals ex."""
    out, x = [], a
    for e0, e1 in ex[max(0, bisect.bisect_right([e for e, _ in ex], a) - 1):]:
        if e0 >= b:
            break
        if e1 <= x:
            continue
        if e0 > x:
            out.append((x, min(e0, b)))
        x = max(x, e1)
    if x < b:
        out.append((x, b))
    return out


def _excluded_at(ex: list[tuple[float, float]], starts: list[float], t: float) -> bool:
    i = bisect.bisect_right(starts, t) - 1
    return i >= 0 and t < ex[i][1]


def blocks(cs: list[tuple[float, float]], wins,
           excl=()) -> dict[str, list[tuple[list[float], float]]]:
    """Per phase, in time order: one (call ms list, seconds in the phase) per
    BLOCK_S block of k6's span (from its first call to its last) that the
    phase holds any of. `excl`: time removed (not jointly Ready) — its calls
    (by completion time, as a call's phase) and its seconds; the block grid
    stays k6's whole span, so nothing excluded = the blocks as before."""
    out: dict[str, list] = {p: [] for p in _names(wins)}
    if not cs:
        return out
    ex = _union(excl)
    starts = [a for a, _ in ex]
    t0 = min(t for t, _ in cs)
    t1 = max(t for t, _ in cs)
    nb = int((t1 - t0) // BLOCK_S) + 1
    vals: dict[tuple[str, int], list[float]] = {}
    for t, v in cs:
        if ex and _excluded_at(ex, starts, t):
            continue
        vals.setdefault((_phase(t, wins), int((t - t0) // BLOCK_S)), []).append(v)
    for k in range(nb):
        a = t0 + k * BLOCK_S
        secs = dict.fromkeys(_names(wins), 0.0)
        for x, y in _minus(a, min(a + BLOCK_S, t1), ex):
            for p, s in _seconds(x, y, wins).items():
                secs[p] += s
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


def phase_stats(cs: list[tuple[float, float]], wins, excl=()) -> dict:
    """Per phase with any k6 second: n, seconds, p95/p99 ms, req/s and their
    half-sample sd (over the time `excl` leaves)."""
    out = {}
    for p, b in blocks(cs, wins, excl).items():
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


def pooled(wins) -> list[tuple[str, float, float]]:
    """run_phases' windows with every churn step named CHURN (precedence
    kept): the gate's coarse phases storm / burst / churn / steady."""
    return [(CHURN if n.startswith("churn:") else n, w0, w1) for n, w0, w1 in wins]


def _ready(m: dict) -> bool:
    """A mem606 sample that saw snowplow Ready: /readyz answered 200 and,
    when the sample could read it, the prewarm-complete signal says the cold
    start is done (readyz itself waits for prewarm since 1.5.29)."""
    pw = m.get("prewarm_complete")
    return m.get("readyz") == READY_STATUS and not (isinstance(pw, dict) and not pw.get("done"))


def _pod_restarts(pods: list[dict] | None) -> dict:
    """The snowplow pod records taken before and after measure: restarts in
    between (same pod), a replaced pod, or unknown / not recorded."""
    if not pods:
        return {"record": "not recorded", "restarts": None}
    before = next((p for p in pods if p.get("phase") == "before"), None)
    after = next((p for p in pods if p.get("phase") == "after"), None)
    if before is None or after is None or before.get("pod") is None or after.get("pod") is None:
        return {"record": "unknown", "restarts": None}
    if before["pod"] != after["pod"]:
        return {"record": "replaced", "restarts": None}
    return {"record": "same pod", "restarts": (after.get("restarts") or 0) - (before.get("restarts") or 0)}


def readiness(mem: list[dict], pods: list[dict] | None, span) -> dict:
    """When this run's snowplow was NOT Ready inside k6's span, and why.

    The signal is mem606's samples (mem_start's interval_s apart): Ready =
    _ready(). The time between two consecutive Ready samples is Ready unless
    a counter reset (num_gc going down) shows a restart between them. A
    not-Ready stretch runs from the last Ready sample before it to the first
    Ready sample after it — for a restart, the first sample whose cold start
    (prewarm) is complete: the recovery tail, measured by the run's own
    signals. Before the first sample and after the last, Ready is unknown:
    excluded too. The pod records count the restarts the samples must place
    (a restart they cannot place leaves its recovery unexcluded: `unlocated`)."""
    rows = sorted((m for m in mem if m.get("kind") == "mem" and m.get("t") is not None),
                  key=lambda m: m["t"])
    start = next((m for m in mem if m.get("kind") == "mem_start"), {})
    interval = start.get("interval_s")
    ex: list[dict] = []

    def add(t0, t1, kind, why, **kw):
        if t1 > t0:
            ex.append({"t0": t0, "t1": t1, "seconds": round(t1 - t0, 1), "kind": kind, "why": why, **kw})

    def close(ep, r_end, t_end):
        restart = ep["reset"]
        pw = (r_end or {}).get("prewarm_complete")
        cold = round(pw["elapsed_ms"] / 1000, 1) if (restart and isinstance(pw, dict)
                                                       and isinstance(pw.get("elapsed_ms"), (int, float))) else None
        codes = ", ".join(str(c) for c in sorted(ep["codes"], key=str))
        seen = (f"{ep['n']} sample(s) not Ready (readyz {codes}"
                f"{'; prewarm not complete in ' + str(ep['pending']) if ep['pending'] else ''})")
        if restart:
            why = ("snowplow restarted (its counters reset) and was not Ready until its cold start "
                   "completed: from the last Ready sample to the first Ready sample with prewarm complete"
                   + (f" (cold start {cold} s by its own prewarm signal)" if cold is not None else "")
                   + f"; {seen}")
        elif r_end is None:
            why = f"snowplow not Ready through the last readiness sample; {seen}"
        else:
            why = f"snowplow not Ready; {seen}"
        add(ep["t0"], t_end, "restart" if restart else "not_ready", why, cold_start_s=cold)

    if not rows:
        if span:
            add(span[0], span[1], "unknown", "no readiness sample: whether snowplow was Ready is unknown")
    else:
        if span and span[0] < rows[0]["t"]:
            add(span[0], rows[0]["t"], "unknown", "before the first readiness sample: Ready is unknown")
        last_ready, gc_ref, ep = None, None, None
        for r in rows:
            ng = load606._num(r.get("num_gc"))
            reset = ng is not None and gc_ref is not None and ng < gc_ref
            if ng is not None:
                gc_ref = ng
            if not _ready(r):
                if ep is None:
                    ep = {"t0": last_ready["t"] if last_ready else r["t"], "codes": set(), "pending": 0,
                          "n": 0, "reset": False}
                ep["n"] += 1
                ep["codes"].add(r.get("readyz"))
                ep["pending"] += 1 if r.get("readyz") == READY_STATUS else 0
                ep["reset"] |= reset
                continue
            if ep is None and reset:
                # restarted between two Ready samples: no sample saw it down
                ep = {"t0": last_ready["t"], "codes": set(), "pending": 0, "n": 0, "reset": True}
            if ep is not None:
                ep["reset"] |= reset
                close(ep, r, r["t"])
                ep = None
            last_ready = r
        if ep is not None:
            close(ep, None, max(rows[-1]["t"], span[1] if span else rows[-1]["t"]))
        if span and rows[-1]["t"] < span[1] and not (ex and ex[-1]["t1"] >= span[1]):
            add(rows[-1]["t"], span[1], "unknown", "after the last readiness sample: Ready is unknown")
    pod = _pod_restarts(pods)
    after = next((p for p in pods or [] if p.get("phase") == "after"), None) or {}
    term = (after.get("last_state") or {}).get("terminated") or {}
    in_samples = sum(1 for e in ex if e["kind"] == "restart")
    count = max(in_samples, pod["restarts"] or 0, 1 if pod["record"] == "replaced" else 0)
    lt = None
    if term:
        t = k6eval.epoch(term.get("finishedAt"))
        lt = {"reason": term.get("reason"), "exit_code": term.get("exitCode"),
              "finished_at": term.get("finishedAt"), "t": t,
              # the kill falls between the last Ready sample and the first that
              # missed it; a node clock may lead mem606's by a little: one interval
              "located": t is not None and any(e["kind"] == "restart" and e["t0"] - (interval or 0) <= t <= e["t1"]
                                              for e in ex)}
    unlocated = []
    if (pod["restarts"] or 0) > in_samples:
        unlocated.append(f"the pod record counts {pod['restarts']} restart(s) during measure, the readiness "
                         f"samples place {in_samples}")
    if (pod["restarts"] or 0) > 0 and lt is not None and not lt["located"]:
        unlocated.append(f"the last termination ({lt['reason']} at {lt['finished_at']}) lies in no restart "
                         f"stretch of the readiness samples")
    if pod["record"] == "replaced" and not in_samples:
        unlocated.append("the pod was replaced during measure and no readiness sample shows when")
    return {"signal": "mem606 /readyz samples (+ prewarm_complete, counter resets); pod records",
            "interval_s": interval, "samples": len(rows), "ready_samples": sum(1 for r in rows if _ready(r)),
            "excluded": ex,
            "restarts": {"restarted": count > 0, "count": count, "in_samples": in_samples, "pod": pod,
                         "last_termination": lt, "unlocated": unlocated}}


def k6_errors(o: dict | None) -> dict:
    """k6's failed requests (http_req_failed points) over its whole stream."""
    pts = ((o or {}).get("points") or {}).get("http_req_failed") or []
    failed = sum(1 for v, _ in pts if v)
    return {"requests": len(pts), "failed": failed, "rate": round(failed / len(pts), 6) if pts else None}


def requests(o: dict | None) -> list[tuple[float, bool]]:
    """k6's requests as (completion epoch, failed), those with a time."""
    if not o:
        return []
    ts = (o.get("times") or {}).get("http_req_failed") or []
    return [(ts[i], bool(v)) for i, (v, _) in enumerate(o["points"].get("http_req_failed", []))
            if i < len(ts) and ts[i] is not None]


def report(k6o: dict | None, browser: dict | None, mem: list[dict], fresh: list[dict] | None,
           churns: list[dict] | None, tops: list[dict], pods: list[dict] | None = None) -> dict:
    """report["customer_latency"] of one run's verdict.json."""
    wins = load606.run_phases(mem, list(fresh or []) + list(churns or []))
    cs = calls(k6o)
    span = [min(t for t, _ in cs), max(t for t, _ in cs)] if cs else None
    return {"block_s": BLOCK_S, "calls": len(cs), "k6_span": span,
            "windows": [[n, w0, w1] for n, w0, w1 in wins],
            "phases": phase_stats(cs, wins),
            # the gate's phases: every churn step pooled while customers are on
            "gate_phases": phase_stats(cs, pooled(wins)),
            "schedule": schedule(k6o, browser, mem, churns),
            # when snowplow was not Ready inside k6's span (the pair removes it from both runs)
            "readiness": readiness(mem, pods, span),
            "k6_errors": k6_errors(k6o),
            "clicks_vs_open_schedule": clicks_vs_schedule(k6o, wins),
            "refresher_completions": refresher_per_phase(mem, wins, span),
            "snowplow_cpu": cpu_per_phase(tops, wins, span)}


def _validity(name, ok, value, bar, why):
    return {"check": f"customer_latency.pair.{name}", "value": value, "bar": bar,
            "result": "PASS" if ok else "FAIL", **({} if ok else {"why": why})}


# a run's own verdict that can be compared (INVALID cannot, unless every
# reason it is INVALID is one the pair handles itself; a missing or unknown
# one is not a run's verdict at all)
COMPARABLE_VERDICTS = ("PASS", "FAIL", "INCONCLUSIVE")


def _measure(x) -> bool:
    """A latency / rate / spread value a ratio can be taken of: a finite
    number, positive (sd: not negative)."""
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and x > 0


def _voiding(v: dict, rd: dict | None) -> list[str]:
    """The reasons this run's own verdict voids the pair: every INVALID
    reason except the two the pair judges itself — a restart the readiness
    record knows (B: FAIL; A: excluded and reported) and k6's whole-run
    error rate (the pair counts it over the jointly-Ready time)."""
    verdict = v.get("verdict")
    if verdict not in COMPARABLE_VERDICTS + ("INVALID",):
        return [f"the run has no verdict a pair can read ({verdict!r})"]
    if verdict != "INVALID":
        return []
    fails = [c for c in v.get("validity") or [] if c.get("result") == "FAIL"]
    if not fails:
        return ["the run is INVALID and states no reason"]
    restarted = (((rd or {}).get("restarts") or {}).get("restarted")) is True
    return [f"{c.get('check')}: {c.get('why') or str(c.get('value')) + ' vs ' + str(c.get('bar'))}"
            for c in fails
            if not (c.get("check") == ERROR_RATE_CHECK or (c.get("check") == RESTART_CHECK and restarted))]


def _wins(r: dict) -> list[tuple[str, float, float]]:
    return [(n, float(w0), float(w1)) for n, w0, w1 in r.get("windows") or []]


def _region(p: str, wins, lo: float, hi: float) -> list[tuple[float, float]]:
    """The parts of [lo, hi) whose phase is p."""
    cuts = sorted({lo, hi} | {x for _, w0, w1 in wins for x in (w0, w1) if lo < x < hi})
    return [(x, y) for x, y in itertools.pairwise(cuts) if _phase(x, wins) == p]


def map_interval(t0: float, t1: float, wf, wt, k6f: float, k6t: float) -> list[tuple[float, float]]:
    """[t0, t1) of one run in the other run's time, by phase-relative time:
    each piece keeps its offset from its phase's start (storm: from its end,
    its start is open; steady: from the boundary before it, else k6's first
    call) and lands in the other run's same phase, clipped to it (a phase
    shorter there has no counterpart past its end). Raises KeyError when the
    other run has no such phase."""
    nf = {n: (w0, w1) for n, w0, w1 in wf} | {"k6": (k6f, k6f)}
    nt = {n: (w0, w1) for n, w0, w1 in wt} | {"k6": (k6t, k6t)}
    bounds = sorted([(k6f, "k6", 0)] + [(x, n, side) for n, w0, w1 in wf
                                        for side, x in enumerate((w0, w1)) if math.isfinite(x)])
    cuts = sorted({t0, t1} | {x for x, _, _ in bounds if t0 < x < t1})
    out = []
    for x0, x1 in itertools.pairwise(cuts):
        p = _phase(x0, wf)
        if p != STEADY:
            name, side = p, 0 if math.isfinite(nf[p][0]) else 1
        else:
            prior = [b for b in bounds if b[0] <= x0]
            _, name, side = prior[-1] if prior else bounds[0]
        af, at = nf[name][side], nt[name][side]
        out += _region(p, wt, at + (x0 - af), at + (x1 - af))
    return out


def joint_exclusion(ra: dict, rb: dict) -> dict:
    """Each run's not-Ready stretches (its readiness record), removed from
    BOTH runs: in its own time as recorded, in the other's mapped by
    phase-relative time. Per item: what, why, and the intervals in A's and
    in B's time (clipped to each run's k6 span); per run the union."""
    spans = {"a": ra.get("k6_span"), "b": rb.get("k6_span")}
    wins = {"a": _wins(ra), "b": _wins(rb)}
    items = []
    for x, y, r in (("a", "b", ra), ("b", "a", rb)):
        for i, e in enumerate(r["readiness"]["excluded"], 1):
            own = [(e["t0"], e["t1"])]
            other = map_interval(e["t0"], e["t1"], wins[x], wins[y], spans[x][0], spans[y][0])
            items.append({"what": f"{x.upper()} {e['kind']} #{i}", "why": e["why"], "kind": e["kind"],
                          "run": x.upper(), x: own, y: other})
    out = {"items": [], "a": [], "b": []}
    for it in items:
        for z in ("a", "b"):
            lo, hi = spans[z]
            it[z] = _union((max(a, lo), min(b, hi)) for a, b in it[z])
        if it["a"] or it["b"]:
            out["items"].append(it)
    for z in ("a", "b"):
        out[z] = _union(iv for it in out["items"] for iv in it[z])
    return out


def _secs_in(ivs, p: str, wins) -> float:
    return sum(_seconds(a, b, wins)[p] for a, b in ivs) if p in _names(wins) else 0.0


def _excluded(p: str, ex: dict, wa, wb) -> dict:
    """Seconds of phase p each run lost, and why (per item; items may overlap,
    the totals are their union)."""
    why = []
    for it in ex["items"]:
        sa, sb = _secs_in(it["a"], p, wa), _secs_in(it["b"], p, wb)
        if sa or sb:
            why.append({"what": it["what"], "why": it["why"], "a_s": round(sa, 1), "b_s": round(sb, 1)})
    return {"a_s": round(_secs_in(ex["a"], p, wa), 1), "b_s": round(_secs_in(ex["b"], p, wb), 1),
            "why": why}


def _ex_text(e: dict) -> str:
    if not e["a_s"] and not e["b_s"]:
        return "excluded: none"
    return (f"excluded a {e['a_s']} s, b {e['b_s']} s: "
            + "; ".join(f"{w['what']} (a {w['a_s']} s, b {w['b_s']} s)" for w in e["why"]))


def raw_from_dir(d: str) -> dict:
    """One run's k6 calls and requests from its collected dir (run606's $OUT)."""
    o = k6eval.read_k6_out(os.path.join(d, "k6"), "k6")
    return {"calls": calls(o), "requests": requests(o)}


def _rate(reqs, ex) -> dict:
    starts = [a for a, _ in ex]
    kept = [f for t, f in reqs if not (ex and _excluded_at(ex, starts, t))]
    n, k = len(kept), sum(kept)
    return {"requests": n, "failed": k, "rate": round(k / n, 6) if n else None}


def pair(va: dict, vb: dict, *, replica: dict | None = None,
         expect_a: str | None = None, expect_b: str | None = None,
         completed: tuple[bool | None, bool | None] = (None, None), raw=(None, None)) -> dict:
    """The paired verdict: B's customer latency per phase against A's (module
    doc). FAIL when B restarted, whatever else; INVALID when the pair cannot be
    compared; FAIL when a cell is past its tolerance; INCONCLUSIVE when a cell
    cannot be decided on the jointly-Ready time. `completed`: whether each
    run's evaluate step ran (the launcher's reading of its run.log); None =
    not known. `raw`: each run's k6 calls and requests (raw_from_dir, or a
    function returning it), read only when some time is excluded."""
    va, vb = va or {}, vb or {}
    ra = (va.get("report") or {}).get("customer_latency")
    rb = (vb.get("report") or {}).get("customer_latency")
    rda, rdb = (ra or {}).get("readiness"), (rb or {}).get("readiness")
    dig = [((v.get("report") or {}).get("image_digest") or {}).get("expected") for v in (va, vb)]
    void = {"a": _voiding(va, rda), "b": _voiding(vb, rdb)}
    validity = [
        _validity("runs_valid", not void["a"] and not void["b"], void,
                  "each run's own verdict carries no INVALID reason but a known restart and k6's "
                  "whole-run error rate (the pair judges those)",
                  "a run whose own measurement is INVALID (or that has no verdict) cannot be compared: "
                  + "; ".join(f"{x.upper()}: {r}" for x in ("a", "b") for r in void[x])),
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
                      "ready_time": "cells over the time both runs' snowplow was Ready (each run's "
                                    "not-Ready stretches removed from both, by phase-relative time); "
                                    "PASS within the bar A's sd scaled to the phase's full length "
                                    "gives, FAIL past the kept sample's own bar, INCONCLUSIVE between",
                      "replica": (replica or {}).get("provenance")}}
    # ── restarts: B's FAIL the pair; A's are recorded (module doc) ──
    rs = {x: ((rd or {}).get("restarts") or {}) for x, rd in (("a", rda), ("b", rdb))}
    out["restarts"] = rs
    b_restart = rs["b"].get("restarted") is True
    lt = rs["b"].get("last_termination") or {}
    out["candidate_restarts"] = {
        "check": "customer_latency.pair.candidate_restarts", "value": rs["b"].get("count"), "bar": "== 0",
        "result": "FAIL" if b_restart else "PASS",
        **({"why": f"B (the candidate) restarted {rs['b'].get('count')}x during measure"
                   + (f": last {lt.get('reason')} (exit {lt.get('exit_code')}) at {lt.get('finished_at')}"
                      if lt else "") + "; any restart of the candidate FAILs the pair"}
           if b_restart else {})}
    if b_restart:
        out["checks"].append(out["candidate_restarts"])
    # B not Ready WITHOUT restarting FAILs the pair too (coordinator 2026-10-24):
    # its users saw errors; cutting its windows would hide that
    spb = (rb or {}).get("k6_span")
    nr = [e for e in (rdb or {}).get("excluded") or [] if e.get("kind") == "not_ready"
          and (not spb or (e["t1"] > spb[0] and e["t0"] < spb[1]))]
    b_not_ready = bool(nr)
    nr_s = round(sum(e["seconds"] for e in nr), 1)
    out["candidate_not_ready"] = {
        "check": "customer_latency.pair.candidate_not_ready",
        "value": {"seconds": nr_s, "stretches": [{k: e[k] for k in ("t0", "t1", "seconds", "why")} for e in nr]},
        "bar": "== 0 s", "result": "FAIL" if b_not_ready else "PASS",
        **({"why": f"B (the candidate) was not Ready without restarting: {len(nr)} stretch(es), {nr_s} s ("
                   + "; ".join(f"{e['seconds']} s: {e['why']}" for e in nr)
                   + "); its users saw it, so it FAILs the pair (its windows are not a way out)"}
           if b_not_ready else {})}
    if b_not_ready:
        out["checks"].append(out["candidate_not_ready"])
    b_fail = b_restart or b_not_ready
    notes = []
    if rs["a"].get("restarted"):
        lta = rs["a"].get("last_termination") or {}
        notes.append(f"A (the baseline) restarted {rs['a'].get('count')}x during measure"
                     + (f" (last {lta.get('reason')}, exit {lta.get('exit_code')}, at {lta.get('finished_at')})"
                        if lta else "")
                     + ": recorded; its not-Ready time is excluded from both runs, it does not void the pair")
    if ra is not None and rb is not None:
        for x, rd in (("a", rda), ("b", rdb)):
            validity.append(_validity(f"readiness[{x}]", rd is not None and "excluded" in rd,
                                      rd is not None, "the run's report carries its readiness record",
                                      "without it the time snowplow was not Ready cannot be excluded"))
        if rda is not None and rdb is not None:
            validity.append(_validity("restarts_placed[a]", not rs["a"].get("unlocated"),
                                      rs["a"].get("unlocated") or "every restart placed",
                                      "each of A's restarts placed in time by its readiness samples",
                                      "a restart of A whose time is unknown: its recovery cannot be excluded"))
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
        ga, gb = ra.get("gate_phases") or {}, rb.get("gate_phases") or {}
        pa, pb = ra.get("phases") or {}, rb.get("phases") or {}
        # ── the jointly-Ready time ──
        ex = {"items": [], "a": [], "b": []}
        if rda is not None and rdb is not None and ra.get("k6_span") and rb.get("k6_span"):
            try:
                ex = joint_exclusion(ra, rb)
            except KeyError as e:
                validity.append(_validity("ready_time_mapped", False, str(e), "each phase in both runs",
                                          f"phase {e} is missing in one run: not-Ready time cannot be mapped"))
        out["exclusion"] = {"a": ex["a"], "b": ex["b"],
                            "items": [{k: it[k] for k in ("what", "why", "a", "b")} for it in ex["items"]]}
        ka, kb, kpa, kpb = ga, gb, pa, pb               # nothing excluded: the runs' own stats
        sick, hs = [], {}
        rates = {x: {"requests": r["k6_errors"]["requests"], "failed": r["k6_errors"]["failed"],
                     "rate": r["k6_errors"]["rate"]} if r.get("k6_errors") else None
                 for x, r in (("a", ra), ("b", rb))}
        if ex["a"] or ex["b"]:
            got = {}
            for x, r, rw in (("a", ra, raw[0]), ("b", rb, raw[1])):
                rw = rw() if callable(rw) else rw
                cs = (rw or {}).get("calls") or []
                same = rw is not None and len(cs) == r.get("calls") and r.get("k6_span") == (
                    [min(t for t, _ in cs), max(t for t, _ in cs)] if cs else None)
                validity.append(_validity(f"raw_calls[{x}]", same,
                                          {"raw": len(cs) if rw is not None else None, "verdict": r.get("calls")},
                                          "the run dir's k6 calls are the ones its verdict measured",
                                          "time must be excluded and this run's own k6 calls are not available"))
                if same:
                    got[x] = rw
            if len(got) == 2:
                wa, wb = _wins(ra), _wins(rb)
                ka = phase_stats(got["a"]["calls"], pooled(wa), ex["a"])
                kb = phase_stats(got["b"]["calls"], pooled(wb), ex["b"])
                kpa = phase_stats(got["a"]["calls"], wa, ex["a"])
                kpb = phase_stats(got["b"]["calls"], wb, ex["b"])
                rates = {x: _rate(got[x]["requests"], ex[x]) for x in ("a", "b")}
                # A-healthy sensitivity (shown only): every phase A restarted in
                # dropped whole from both runs, so a reader sees how much A's
                # pre-crash minutes (Ready, so kept) move the ratio
                sick = [n for n in _names(wa) if any(_secs_in(it["a"], n, wa) > 0 for it in ex["items"]
                                                     if it["run"] == "A" and it["kind"] == "restart")]
                if sick:
                    hx = {x: _union(list(ex[x]) + [iv for n in sick
                                                   for iv in _region(n, _wins(r), *r["k6_span"])])
                          for x, r in (("a", ra), ("b", rb))}
                    hs = {"pooled": (phase_stats(got["a"]["calls"], pooled(wa), hx["a"]),
                                     phase_stats(got["b"]["calls"], pooled(wb), hx["b"])),
                          "steps": (phase_stats(got["a"]["calls"], wa, hx["a"]),
                                    phase_stats(got["b"]["calls"], wb, hx["b"]))}
            else:
                ka = kb = kpa = kpb = None
                rates = {"a": None, "b": None}
        # ── k6 errors: B's over the jointly-Ready time void; A's reported ──
        th = k6eval.K6_THRESHOLDS["max_k6_error_rate"]
        rb_ = rates["b"]
        validity.append(_validity(
            "k6_error_rate[b]", rb_ is not None and rb_["rate"] is not None and rb_["rate"] <= th, rb_,
            f"<= {th} of B's k6 requests failed, inside the jointly-Ready time",
            "B's k6 requests failed beyond the bar while both runs were Ready: the load did not reach "
            "snowplow as scheduled" if rb_ and rb_["rate"] is not None else "B's error rate is unknown"))

        def summary_rate(v):
            return next((c.get("value") for c in v.get("validity") or [] if c.get("check") == ERROR_RATE_CHECK),
                        None)
        out["k6_error_rate"] = {x: {"jointly_ready": rates[x],
                                    "whole_run_stream": (r.get("k6_errors") if r else None),
                                    "whole_run_summary": summary_rate(v)}
                                for x, r, v in (("a", ra, va), ("b", rb, vb))}
        notes.append(f"A's k6 error rate: whole run {out['k6_error_rate']['a']['whole_run_summary']} "
                     f"(k6 summary), {(out['k6_error_rate']['a']['whole_run_stream'] or {}).get('rate')} "
                     f"(stream); jointly Ready {(rates['a'] or {}).get('rate')} — reported, never voids")
        for p in REQUIRED:
            if not (ga.get(p) or {}).get("n"):
                validity.append(_validity(f"judged[{p}]", False, 0, "> 0 k6 calls in A",
                                          f"A has no k6 call in {p}: the pair judges nothing there"))
        judged = [p for p in JUDGED if (ga.get(p) or {}).get("n")]
        cells = [(p, m) for p in judged for m in METRICS]
        z = NormalDist().inv_cdf(1 - FAMILY_ALPHA / len(cells)) if cells else None
        out["method"].update(cells=len(cells), z=round(z, 4) if z else None, judged=judged,
                             shown=list(SHOWN) + ["each churn step"])
        wa, wb = pooled(_wins(ra)), pooled(_wins(rb))
        for p in judged:
            if not (gb.get(p) or {}).get("n"):
                validity.append(_validity(f"measured[{p}]", False, 0, "> 0 k6 calls in B",
                                          f"B has no k6 call in {p}: the cell cannot be compared"))
                continue
            if ka is None:
                continue                              # raw_calls INVALID: nothing to compute on
            e = _excluded(p, ex, wa, wb)
            for m in METRICS:
                c = _cell(p, m, ka.get(p), kb.get(p), z, replica, judged=True,
                          full_s=ga[p].get("seconds"), excluded=e)
                if c is None:
                    kpa_, kpb_ = ka.get(p) or {}, kb.get(p) or {}
                    validity.append(_validity(f"spread[{p}].{m}", False,
                                              {"a": kpa_.get(m), "b": kpb_.get(m),
                                               "a_half_sd": (kpa_.get("half_sd") or {}).get(m),
                                               "replica": _replica(replica, p, m)},
                                              "finite positive A, B, A's half-sample sd >= 0, replica >= 1",
                                              f"{p} {m}: not a measurement, no tolerance can be derived"))
                    continue
                _a_healthy(c, sick, hs.get("pooled"), out, notes)
                out["cells"].append(c)
                out["checks"].append({
                    "check": f"customer_latency.pair[{p}].{m}",
                    "value": {"a": c["a"], "b": c["b"], "ratio": c["ratio"]},
                    "bar": f"ratio {c['bar']} (within {c['within']} from A's sd {c['a_half_sd']}; "
                           f"replica {c['replica']})",
                    "excluded": e,
                    "result": c["result"],
                    **({} if c["result"] == "PASS" else {"why": c["why"]})})
        # shown, never judged: burst, and each churn step on its own
        if ka is not None:
            sw = [(p, ga, gb, ka, kb, wa, wb) for p in SHOWN] + [
                (p, pa, pb, kpa, kpb, _wins(ra), _wins(rb)) for p in pa if p.startswith("churn:")]
            for p, fa, fb, sa_all, sb_all, xa, xb in sw:
                if not (z and (fa.get(p) or {}).get("n") and (fb.get(p) or {}).get("n")):
                    continue
                e = _excluded(p, ex, xa, xb)
                for m in METRICS:
                    c = _cell(p, m, sa_all.get(p), sb_all.get(p), z, replica, judged=False,
                              full_s=fa[p].get("seconds"), excluded=e)
                    if c is None:
                        # still the ratio when both values are measurements;
                        # no bar when A's spread is not (a short step)
                        a, b = (sa_all.get(p) or {}).get(m), (sb_all.get(p) or {}).get(m)
                        r = round(b / a, 4) if _measure(a) and _measure(b) else None
                        c = {"phase": p, "metric": m, "judged": False, "a": a, "b": b, "ratio": r,
                             "bar": "n/a", "within": None,
                             "a_half_sd": ((sa_all.get(p) or {}).get("half_sd") or {}).get(m),
                             "replica": _replica(replica, p, m), "result": "INFO", "excluded": e,
                             "line": f"INFO {p} {m}: B/A = {r} (n/a, not judged: no spread for A here; "
                                     f"a {a}, b {b}) | {_ex_text(e)}"}
                    _a_healthy(c, sick, hs.get("steps" if p.startswith("churn:") else "pooled"), out, notes)
                    out["cells"].append(c)
    out["notes"] = notes
    # (no judged cell at all is already INVALID: judged[storm] / judged[churn])
    results = [c["result"] for c in out["checks"]]
    out["verdict"] = ("FAIL" if b_fail else
                      "INVALID" if any(c["result"] == "FAIL" for c in validity) else
                      "FAIL" if "FAIL" in results else
                      INCONCLUSIVE if INCONCLUSIVE in results else "PASS")
    out["invalid_reasons"] = [f"{c['check']}: {c['why']}" for c in validity if c["result"] == "FAIL"]
    out["fail_reasons"] = [f"{c['check']}: {c['why']}" for c in out["checks"] if c["result"] == "FAIL"]
    # beside the verdict: both runs' per-phase reports, side by side
    for key in ("gate_phases", "phases"):
        out[key] = {p: {"a": ((ra or {}).get(key) or {}).get(p), "b": ((rb or {}).get(key) or {}).get(p)}
                    for p in dict.fromkeys(list((ra or {}).get(key) or {}) + list((rb or {}).get(key) or {}))}
    for k in ("refresher_completions", "snowplow_cpu", "clicks_vs_open_schedule", "readiness"):
        out[k] = {"a": (ra or {}).get(k), "b": (rb or {}).get(k)}
    return out


def _a_healthy(c: dict, sick: list[str], stats, out: dict, notes: list[str]) -> None:
    """The cell's A-healthy sensitivity (shown, never judged): the same cell
    with every phase A restarted in dropped whole from both runs. A NOTE when
    B passes a judged cell that A restarted inside."""
    p, m = c["phase"], c["metric"]
    inside = [n for n in sick if n == p or (p == CHURN and n.startswith("churn:"))]
    if not inside:
        h = {"phases_dropped": [], "ratio": c.get("ratio"), "same_as_cell": True}
        line = f"INFO A-healthy {p} {m}: = the cell (A had no restart in {p})"
    else:
        sa, sb = ((stats or ({}, {}))[0].get(p) or {}), ((stats or ({}, {}))[1].get(p) or {})
        a, b = sa.get(m), sb.get(m)
        r = round(b / a, 4) if _measure(a) and _measure(b) else None
        h = {"phases_dropped": inside, "a": a, "b": b, "ratio": r, "a_kept_s": sa.get("seconds"),
             "b_kept_s": sb.get("seconds"), "same_as_cell": False}
        line = (f"INFO A-healthy {p} {m}: B/A = {r} without {', '.join(inside)} (A restarted there; "
                f"a {a}, b {b} on {sa.get('seconds')} s of A), vs {c.get('ratio')} in the cell; shown, not judged")
        if c["judged"] and c["result"] == "PASS":
            notes.append(f"B passes {p} {m} (B/A {c.get('ratio')}) while A restarted inside it "
                         f"({', '.join(inside)}): A's pre-crash minutes count as Ready there; "
                         f"A-healthy B/A = {r}")
    c["a_healthy"] = dict(h, line=line)
    out.setdefault("a_healthy_lines", []).append(line)


def _replica(replica, p, m):
    return (((replica or {}).get("phases") or {}).get(p) or {}).get(m)


def _cell(p, m, sa, sb, z, replica, *, judged, full_s=None, excluded=None) -> dict | None:
    """One cell: B / A against max(within, replica), over the jointly-Ready
    time. None when A, B, A's sd or the replica is not a finite positive
    measurement (an A of 0, a negative p95, an infinite spread or replica
    would make the ratio or the bar meaningless: a +Inf spread passed
    anything).

    `full_s`: the phase's seconds in A with nothing excluded. When the kept
    sample is shorter, A's sd on it is scaled by sqrt(kept / full) (the
    1/sqrt(n) law) to the sd a full-length sample would have: PASS needs B / A
    within THAT bar, FAIL needs B / A past the kept sample's own bar; in
    between, the shortened overlap decides: INCONCLUSIVE. A kept sample with
    no calls, or no spread left, is INCONCLUSIVE."""
    e = excluded or {"a_s": 0.0, "b_s": 0.0, "why": []}
    sa, sb = sa or {}, sb or {}
    sd, a, b, rp = (sa.get("half_sd") or {}).get(m), sa.get(m), sb.get(m), _replica(replica, p, m)
    kept = sa.get("seconds") if sa else 0.0          # no stats left: every second excluded
    shrunk = full_s is not None and kept is not None and kept < full_s
    base = {"phase": p, "metric": m, "judged": judged, "a": a, "b": b, "a_half_sd": sd, "replica": rp,
            "kept_s": kept, "full_s": full_s, "excluded": e}
    if shrunk and (not sa.get("n") or not sb.get("n") or (sd is None and _measure(a) and _measure(b))):
        result = INCONCLUSIVE if judged else "INFO"
        r = round(b / a, 4) if _measure(a) and _measure(b) else None
        why = (f"{p} {m}: too little jointly-Ready time ({kept} of {full_s} s of A's) — "
               + ("no k6 call left in a run" if not sa.get("n") or not sb.get("n")
                  else "too few blocks left for A's spread"))
        return {**base, "ratio": r, "bar": "n/a", "fail_bar": "n/a", "within": None, "result": result,
                "why": why, "line": f"{result} {why}; B/A = {r} | {_ex_text(e)}"}
    if not (_measure(a) and _measure(b) and (_measure(sd) or sd == 0)
            and (rp is None or (_measure(rp) and rp >= 1))):
        return None
    within_kept = math.exp(z * math.sqrt(2) * sd)
    within = math.exp(z * math.sqrt(2) * sd * (math.sqrt(kept / full_s) if shrunk else 1.0))
    tol, tol_fail = max(within, rp or 1.0), max(within_kept, rp or 1.0)
    lower = m in LOWER_IS_WORSE
    bar = round(1 / tol, 4) if lower else round(tol, 4)
    fail_bar = round(1 / tol_fail, 4) if lower else round(tol_fail, 4)
    ratio = round(b / a, 4)
    if lower:
        res = "PASS" if ratio >= bar else "FAIL" if ratio < fail_bar else INCONCLUSIVE
    else:
        res = "PASS" if ratio <= bar else "FAIL" if ratio > fail_bar else INCONCLUSIVE
    result = res if judged else "INFO"
    op = ">=" if lower else "<="
    short = (f"; on {kept} of {full_s} s jointly Ready: PASS {op} {bar} (full-length sd), "
             f"FAIL past {fail_bar} (kept sample's)" if shrunk else "")
    why = (f"{p} {m}: B is {ratio}x A, beyond the spread ({op} {bar})" if res == "FAIL" else
           f"{p} {m}: B is {ratio}x A, between the full-length bar {bar} and the kept sample's {fail_bar}: "
           f"the shortened jointly-Ready time decides it" if res == INCONCLUSIVE else None)
    return {**base, "ratio": ratio, "bar": f"{op} {bar}", "fail_bar": f"{op} {fail_bar}",
            "within": round(within, 4), "within_kept": round(within_kept, 4), "result": result, "why": why,
            "line": f"{result} {p} {m}: B/A = {ratio} ({op} {bar}"
                    f"{'' if judged else ', not judged'}; a {a}, b {b}; within {round(within, 4)}, "
                    f"replica {rp}{short}) | {_ex_text(e)}"}


def replica_ratios(r1: dict, r2: dict, provenance: dict) -> dict:
    """The replica file: per gate phase both reports measured, |log| ratio
    per metric (either direction: the spread's size)."""
    phases = {}
    for p, s1 in r1["gate_phases"].items():
        s2 = r2["gate_phases"].get(p)
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
                  [r for r in fr if r.get("kind") == "churn"], tops,
                  [r for r in jsonl("pod-*.jsonl") if r.get("kind") == "snowplow_pod"])


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
        pr.add_argument(f"--{x}-dir", default=None,
                        help=f"{x.upper()}'s collected run dir (its k6/ stream: read when not-Ready time "
                             f"is excluded); default: the directory of --{x}")
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
    dirs = [d or os.path.dirname(os.path.abspath(v)) for d, v in ((a.a_dir, a.a), (a.b_dir, a.b))]
    va, vb = load(a.a), load(a.b)
    try:
        out = pair(va, vb, replica=load_replica(a.replica) if a.replica else None,
                   expect_a=a.expect_a, expect_b=a.expect_b, completed=done,
                   raw=tuple((lambda d=d: raw_from_dir(d)) for d in dirs))
    except Exception as e:  # noqa: BLE001 — a crash must read INVALID, never exit 1 = FAIL
        out = {"kind": "customer_latency_pair", "verdict": "INVALID", "checks": [], "cells": [],
               "invalid_reasons": [f"the paired verdict could not be computed: {type(e).__name__}: {e}"]}
        # ... unless the candidate restarted or was not Ready: that FAILs the pair, never hidden
        rs = (((((vb or {}).get("report") or {}).get("customer_latency") or {}).get("readiness") or {})
              .get("restarts") or {})
        nr = [e for e in (((((vb or {}).get("report") or {}).get("customer_latency") or {}).get("readiness")
                           or {}).get("excluded") or []) if e.get("kind") == "not_ready"]
        if rs.get("restarted") is True or nr:
            out["verdict"] = "FAIL"
            out["fail_reasons"] = ([f"customer_latency.pair.candidate_restarts: B (the candidate) restarted "
                                    f"{rs.get('count')}x during measure"] if rs.get("restarted") is True else []) + (
                [f"customer_latency.pair.candidate_not_ready: B (the candidate) was not Ready for "
                 f"{round(sum(e['seconds'] for e in nr), 1)} s without restarting"] if nr else [])
    json.dump(out, sys.stdout, indent=1, default=str)
    print()
    return {"PASS": 0, "FAIL": 1, "INVALID": 3, INCONCLUSIVE: 4}[out["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
