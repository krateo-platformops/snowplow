"""#1390 item 4: run 26e's replica statistics ran for hours (2 h 17 m on one core, not done) because half_sd
re-sorted every half-sample's concatenated calls, HALF_SAMPLES times per phase and metric. The new half_sd sorts
each block once and selects the two order statistics _pct needs from the picked blocks. Its numbers must be
IDENTICAL to the former implementation: the former code is kept here, verbatim (a7b02456 bench/custlat.py
_pct / _stat / half_sd), and compared bit for bit on run 14's real calls and on randomized blocks.
"""

from __future__ import annotations

import gzip
import json
import math
import random
import statistics
from pathlib import Path

import pytest

from bench import custlat

FIX = Path(__file__).resolve().parent / "fixtures"


# ─── the former implementation, verbatim (a7b02456 bench/custlat.py) ────────

def _old_pct(xs, p):
    if not xs:
        return None
    v = sorted(xs)
    k = (len(v) - 1) * p / 100.0
    lo, hi = int(k), min(int(k) + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (k - lo)


def _old_stat(bl, metric):
    if metric == "req_s":
        secs = sum(s for _, s in bl)
        return sum(len(v) for v, _ in bl) / secs if secs > 0 else None
    return _old_pct([x for v, _ in bl for x in v], {"p95_ms": 95, "p99_ms": 99}[metric])


def _old_half_sd(bl, metric, n=custlat.HALF_SAMPLES, seed=custlat.HALF_SEED):
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
        sa, sb = _old_stat(a, metric), _old_stat(b, metric)
        if sa and sb:
            logs.append(math.log(sa / sb))
    return statistics.pstdev(logs) / 2 if len(logs) >= n // 2 else None


def _same(a, b) -> bool:
    """Bit for bit: both None, or the same float (repr too: -0.0 and 0.0 differ)."""
    return (a is None and b is None) or (a is not None and b is not None and repr(a) == repr(b))


# ─── run 14's real calls ────────────────────────────────────────────────────

def _run14_blocks():
    with gzip.open(FIX / "customer_latency_run14_every50.json.gz", "rt") as fh:
        d = json.load(fh)
    cs = [(d["t0"] + off, v) for off, v in d["calls"]]
    t0, t1 = cs[0][0], max(t for t, _ in cs)
    third = (t1 - t0) / 3
    wins = [("storm", float("-inf"), t0 + third), ("churn:x", t0 + third, t0 + 2 * third)]
    return {p: b for p, b in custlat.blocks(cs, wins).items() if b}


@pytest.mark.parametrize("metric", custlat.METRICS)
def test_half_sd_is_identical_on_run14s_calls(metric):
    phases = _run14_blocks()
    assert len(phases) == 3 and all(len(b) >= 4 for b in phases.values())
    for phase, bl in phases.items():
        old, new = _old_half_sd(bl, metric), custlat.half_sd(bl, metric)
        assert old is not None and _same(old, new), (phase, metric, old, new)


def test_phase_stats_is_identical_on_run14s_calls():
    """The whole per-phase record (n, seconds, blocks, p95/p99/req_s and every half_sd), as before."""
    with gzip.open(FIX / "customer_latency_run14_every50.json.gz", "rt") as fh:
        d = json.load(fh)
    cs = [(d["t0"] + off, v) for off, v in d["calls"]]
    t0, t1 = cs[0][0], max(t for t, _ in cs)
    wins = [("storm", float("-inf"), t0 + (t1 - t0) / 2)]
    got = custlat.phase_stats(cs, wins)
    for p, b in custlat.blocks(cs, wins).items():
        if not b:
            continue
        want = {m: (round(s, 4) if (s := _old_half_sd(b, m)) is not None else None) for m in custlat.METRICS}
        assert got[p]["half_sd"] == want, p


# ─── randomized against the former code ─────────────────────────────────────

def _random_blocks(rnd: random.Random):
    nb = rnd.randint(0, 40)
    ties = rnd.random() < 0.5
    out = []
    for _ in range(nb):
        size = rnd.choice([0, 0, 1, 2, 3, rnd.randint(4, 60)])
        vals = [float(rnd.randint(1, 12)) if ties else rnd.lognormvariate(4, 1) for _ in range(size)]
        out.append((vals, rnd.choice([0.0, 2.5, 10.0, 10.0, 10.0])))
    return out


@pytest.mark.parametrize("case", range(300))
def test_half_sd_is_identical_on_random_blocks(case):
    rnd = random.Random(1390 * 1000 + case)
    bl = _random_blocks(rnd)
    n, seed = rnd.choice([1, 2, 7, 40]), rnd.randint(0, 10**6)

    def outcome(f, metric):
        try:
            return ("value", f(bl, metric, n, seed))
        except statistics.StatisticsError as e:          # n=1 with no usable draw: the former code raised too
            return ("raised", str(e))
    for metric in custlat.METRICS:
        old, new = outcome(_old_half_sd, metric), outcome(custlat.half_sd, metric)
        assert old[0] == new[0] and (_same(old[1], new[1]) if old[0] == "value" else old[1] == new[1]), \
            (case, metric, old, new)


def test_the_percentiles_never_sort_a_half_samples_concatenation(monkeypatch):
    """The cost the fix removes, counted (no wall clock): the former half_sd called _stat on a concatenation
    for each half of each draw; the new one calls it for req_s only."""
    bl = _run14_blocks()["storm"]
    calls = []
    real = custlat._stat
    monkeypatch.setattr(custlat, "_stat", lambda b, m: calls.append(m) or real(b, m))
    for m in ("p95_ms", "p99_ms"):
        custlat.half_sd(bl, m, n=50)
    assert calls == []
    custlat.half_sd(bl, "req_s", n=50)
    assert calls == ["req_s"] * 100
